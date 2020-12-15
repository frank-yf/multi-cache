package com.github.yuefei7746.multicache.support;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.yuefei7746.multicache.MultiCacheProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.cache.support.NullValue;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * @author yuefei7746
 */
public class MultiCache extends AbstractValueAdaptingCache {

    private static final Logger log = LoggerFactory.getLogger(MultiCache.class);

    private static final ThreadLocalRandom expireRandom = ThreadLocalRandom.current();

    private final String name;

    private final ReadWriteLock lock;

    private final RedisTemplate<Object, Object> redisTemplate;

    private final ValueOperations<Object, Object> opsForValue;


    private final Cache<Object, Object> caffeineCache;

    private final String cachePrefix;

    private final String redisTopicKey;

    private long minRedisExpire;
    private long maxRedisExpire;

    private final ConversionService redisKeyConverter;

    public MultiCache(String name,
                      RedisTemplate<Object, Object> redisTemplate,
                      MultiCacheProperty multiCacheProperty) {
        super(multiCacheProperty.isCacheNullValues());
        this.name = name;
        this.lock = new ReentrantReadWriteLock();
        this.redisTemplate = redisTemplate;
        this.opsForValue = redisTemplate.opsForValue();
        this.caffeineCache = multiCacheProperty.getCaffeine().createCache();
        this.cachePrefix = generateKeyPrefix(name, multiCacheProperty.getCachePrefix());

        MultiCacheProperty.RedisProperty redisProperty = multiCacheProperty.getRedis();
        this.redisTopicKey = redisProperty.getTopic();
        initRedisExpire(name, redisProperty);
        this.redisKeyConverter = generateRedisKeyConverter();
    }

    private static String generateKeyPrefix(String cacheName, @Nullable String configPrefix) {
        StringJoiner joiner = new StringJoiner(":", "", "::");
        if (configPrefix != null && configPrefix.length() > 0) {
            joiner.add(configPrefix);
        }
        return joiner.add(cacheName).toString();
    }

    private void initRedisExpire(String cacheName, MultiCacheProperty.RedisProperty redisProperty) {
        Duration duration = redisProperty.getExpires().getOrDefault(cacheName, redisProperty.getDefaultExpiration());
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalStateException("Invalid expire time in Redis set");
        }
        long expire = duration.toMillis();
        if (expire > 0) {
            minRedisExpire = redisProperty.offsetToLeft(expire);
            maxRedisExpire = redisProperty.offsetToRight(expire);
        }
    }

    /**
     * @see RedisCacheConfiguration#defaultCacheConfig(ClassLoader)
     */
    private static ConversionService generateRedisKeyConverter() {
        DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
        conversionService.addConverter(String.class, byte[].class, source -> source.getBytes(StandardCharsets.UTF_8));
        conversionService.addConverter(SimpleKey.class, String.class, SimpleKey::toString);
        return conversionService;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Object getNativeCache() {
        return this;
    }

    @Override
    protected Object lookup(Object key) {
        Lock rl = lock.readLock();
        try {
            rl.lock();

            // 此处只锁定读锁，写入时的同步交给 caffeine cache
            return caffeineCache.get(key, k -> opsForValue.get(createRedisKey(k)));
        } finally {
            rl.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> valueLoader) {
        Lock rl = lock.readLock();
        try {
            rl.lock();

            // 此处只锁定读锁，写入时的同步交给 caffeine cache
            Object storeValue = caffeineCache.get(key, new LoadFunction(valueLoader));
            return (T) fromStoreValue(storeValue);
        } finally {
            rl.unlock();
        }
    }

    @Override
    public void put(Object key, @Nullable Object value) {
        Lock wl = this.lock.writeLock();
        try {
            wl.lock();

            Object storeValue = toStoreValue(value);
            setToRedis(createRedisKey(key), storeValue);
            pushRefresh(key, storeValue);
            caffeineCache.put(key, storeValue);
        } finally {
            wl.unlock();
        }
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, final @Nullable Object value) {
        PutIfAbsentFunction callable = new PutIfAbsentFunction(value);
        Object result = caffeineCache.get(key, callable);
        return (callable.called ? null : toValueWrapper(result));
    }

    @Override
    public void evict(Object key) {
        Lock wl = this.lock.writeLock();
        try {
            wl.lock();

            redisTemplate.delete(createRedisKey(key));
            pushEvict(key);
            caffeineCache.invalidate(key);
        } finally {
            wl.unlock();
        }
    }

    @Override
    public boolean evictIfPresent(Object key) {
        Object lookup = lookup(key);
        if (lookup == null) {
            return false;
        }
        Lock wl = this.lock.writeLock();
        try {
            wl.lock();

            redisTemplate.delete(createRedisKey(key));
            pushEvict(key);
            caffeineCache.invalidate(key);
        } finally {
            wl.unlock();
        }
        return true;
    }

    @Override
    public void clear() {
        Lock wl = this.lock.writeLock();
        try {
            wl.lock();

            redisTemplate.delete(createRedisKey("*"));
            pushEvict(null);
            caffeineCache.invalidateAll();
        } finally {
            wl.unlock();
        }
    }

    public void pullMessage(TopicMessage msg) {
        Object key = msg.getKey();
        Object storeValue = msg.getValue();
        if (key == null) {
            log.debug("clear all local cache");
            caffeineCache.invalidateAll();
        } else if (storeValue == null) {
            log.debug("clear local cache, the key is : {}", key);
            caffeineCache.invalidate(key);
        } else {
            log.debug("refresh local cache, key : {}, value : {}",
                    key, fromStoreValue(storeValue));
            caffeineCache.put(key, storeValue);
        }
    }

    private void setToRedis(String redisKey, Object storeValue) {
        if (storeValue == NullValue.INSTANCE) {
            return;
        }
        opsForValue.set(redisKey, storeValue, getRedisExpire(), TimeUnit.MILLISECONDS);
    }

    /**
     * 使用 topic 确保 Redis 与 Caffeine 的最终一致性
     */
    private void pushEvict(@Nullable Object key) {
        redisTemplate.convertAndSend(redisTopicKey, TopicMessage.create(this.name, key));
    }

    /**
     * 使用 topic 确保 Redis 与 Caffeine 的最终一致性
     */
    private void pushRefresh(@Nullable Object key, Object value) {
        redisTemplate.convertAndSend(redisTopicKey, TopicMessage.create(this.name, key, value));
    }

    private String createRedisKey(Object key) {
        return cachePrefix.concat(convertKey(key));
    }

    private long getRedisExpire() {
        return expireRandom.nextLong(minRedisExpire, maxRedisExpire);
    }

    /**
     * copy for org.springframework.data.redis.cache.RedisCache#convertKey(java.lang.Object)
     */
    private String convertKey(Object key) {

        if (key instanceof String) {
            return (String) key;
        }

        TypeDescriptor source = TypeDescriptor.valueOf(key.getClass());

        if (redisKeyConverter.canConvert(source, TypeDescriptor.valueOf(String.class))) {
            try {
                return Objects.requireNonNull(redisKeyConverter.convert(key, String.class));
            } catch (ConversionFailedException e) {

                // may fail if the given key is a collection
                if (source.isArray() || source.isCollection() || source.isMap()) {
                    return convertCollectionLikeOrMapKey(key, source);
                }

                throw e;
            }
        }

        Method toString = ReflectionUtils.findMethod(key.getClass(), "toString");

        if (toString != null && !Object.class.equals(toString.getDeclaringClass())) {
            return key.toString();
        }

        throw new IllegalStateException(String.format(
                "Cannot convert cache key %s to String. Please register a suitable Converter via 'RedisCacheConfiguration.configureKeyConverters(...)' or override '%s.toString()'.",
                source, key.getClass().getSimpleName()));
    }

    /**
     * copy for org.springframework.data.redis.cache.RedisCache#convertCollectionLikeOrMapKey(java.lang.Object, org.springframework.core.convert.TypeDescriptor)
     */
    private String convertCollectionLikeOrMapKey(Object key, TypeDescriptor source) {

        if (source.isMap()) {

            StringBuilder target = new StringBuilder("{");

            for (Map.Entry<?, ?> entry : ((Map<?, ?>) key).entrySet()) {
                target.append(convertKey(entry.getKey())).append("=").append(convertKey(entry.getValue()));
            }
            target.append("}");

            return target.toString();
        } else if (source.isCollection() || source.isArray()) {

            StringJoiner sj = new StringJoiner(",", "[", "]");

            Collection<?> collection = source.isCollection() ? (Collection<?>) key
                    : Arrays.asList(ObjectUtils.toObjectArray(key));

            for (Object val : collection) {
                sj.add(convertKey(val));
            }
            return sj.toString();
        }

        throw new IllegalArgumentException(String.format("Cannot convert cache key %s to String.", key));
    }


    /**
     * Like for org.springframework.cache.caffeine.CaffeineCache.PutIfAbsentFunction
     */
    private class PutIfAbsentFunction implements Function<Object, Object> {

        @Nullable
        private final Object value;

        private boolean called;

        public PutIfAbsentFunction(@Nullable Object value) {
            this.value = value;
        }

        /**
         * caffeine 已经保证了会同步调用，不需要多余的同步控制
         */
        @Override
        public Object apply(Object key) {
            this.called = true;
            return new LoadFunction(() -> value).apply(key);
        }
    }

    /**
     * Like for org.springframework.cache.caffeine.CaffeineCache.LoadFunction
     */
    private class LoadFunction implements Function<Object, Object> {

        private final Callable<?> valueLoader;

        public LoadFunction(Callable<?> valueLoader) {
            this.valueLoader = valueLoader;
        }

        /**
         * caffeine 已经保证了会同步调用，不需要多余的同步控制
         */
        @Override
        public Object apply(Object k) {
            String redisKey = createRedisKey(k);
            try {
                Object storeValue = opsForValue.get(redisKey);
                if (storeValue != null) {
                    pushRefresh(k, storeValue);
                    return storeValue;
                }

                Object calledValue = toStoreValue(this.valueLoader.call());
                setToRedis(redisKey, calledValue);
                pushRefresh(k, calledValue);

                return calledValue;
            } catch (Exception ex) {
                throw new ValueRetrievalException(k, this.valueLoader, ex);
            }
        }

    }

}
