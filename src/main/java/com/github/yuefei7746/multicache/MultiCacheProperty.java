package com.github.yuefei7746.multicache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author yuefei7746
 */
@Data
@ConfigurationProperties(prefix = "spring.cache.multi")
public class MultiCacheProperty {

    private Set<String> cacheNames = new HashSet<>();

    /**
     * 是否需要存储空值，设为 true 以防止缓存穿透。默认值：true
     * <p>
     * 个人认为空值没有提交到 Redis 的必要，内存缓存已经足够把空值请求量控制到一定水平以下了。
     * 但如果需要修改：MultiCache#setToRedis(java.lang.String, java.lang.Object)
     */
    private boolean cacheNullValues = true;

    /**
     * 是否动态根据 cacheName 创建 Cache 的实现。默认值：true
     */
    private boolean dynamic = true;

    /**
     * 全局缓存 key 的前缀
     */
    private String cachePrefix;

    private RedisProperty redis = new RedisProperty();

    private CaffeineProperty caffeine = new CaffeineProperty();

    @Data
    public static class RedisProperty {

        /**
         * 全局过期时间，默认不过期
         */
        private Duration defaultExpiration = Duration.ofMinutes(10);

        /**
         * 缓存过期时间偏移量，redis 的缓存将根据偏移量的左右范围随机生成，以防止缓存雪崩。默认值：0.2
         */
        private double expireOffset = 0.2;

        /**
         * 每个cacheName的过期时间，单位毫秒，优先级比defaultExpiration高
         */
        private Map<String, Duration> expires = new HashMap<>();

        /**
         * 缓存更新时通知其他节点的topic名称
         */
        private String topic = "cache:multi:topic";

        public long offsetToLeft(long expire) {
            return BigDecimal.valueOf(expire)
                    .multiply(BigDecimal.valueOf(1 - expireOffset))
                    .longValue();
        }

        public long offsetToRight(long expire) {
            return BigDecimal.valueOf(expire)
                    .multiply(BigDecimal.valueOf(1 + expireOffset))
                    .longValue();
        }

    }

    @Data
    public static class CaffeineProperty {

        /**
         * 访问后过期时间
         * <p>
         * 在最后一次访问或者写入后开始计时，在指定的时间后过期。假如一直有请求访问该key，那么这个缓存将一直不会过期。
         */
        private Duration expireAfterAccess = Duration.ZERO;

        /**
         * 写入后过期时间，默认值：3 分钟
         * <p>
         * 在最后一次写入缓存后开始计时，在指定的时间后过期。
         */
        private Duration expireAfterWrite = Duration.ofMinutes(3);

        /**
         * 写入后刷新时间
         * <p>
         * 在最后一次写入缓存后开始计时，在指定的时间后刷新。
         * 如果使用refreshAfterWrite配置,必须指定一个CacheLoader
         */
        private Duration refreshAfterWrite = Duration.ZERO;

        /**
         * 初始化大小，默认值：10
         */
        private int initialCapacity = 10;

        /**
         * 最大缓存对象个数，默认值：1000
         */
        private long maximumSize = 1000;

        /**
         * @deprecated 由于权重需要缓存对象来提供，对于使用spring cache这种场景不是很适合，所以暂不支持配置
         */
        @Deprecated
        private long maximumWeight;

        public Cache<Object, Object> createCache() {
            Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
            if (!expireAfterAccess.isZero()) {
                cacheBuilder.expireAfterAccess(expireAfterAccess);
            }
            if (!expireAfterWrite.isZero()) {
                cacheBuilder.expireAfterWrite(expireAfterWrite);
            }
            if (initialCapacity > 0) {
                cacheBuilder.initialCapacity(initialCapacity);
            }
            if (maximumSize > 0) {
                cacheBuilder.maximumSize(maximumSize);
            }
            if (!refreshAfterWrite.isZero()) {
                cacheBuilder.refreshAfterWrite(refreshAfterWrite);
            }
            return cacheBuilder.build();
        }

    }

}
