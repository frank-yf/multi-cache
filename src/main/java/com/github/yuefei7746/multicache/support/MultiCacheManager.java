package com.github.yuefei7746.multicache.support;

import com.github.yuefei7746.multicache.MultiCacheProperty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * @author yuefei7746
 */
@Slf4j
public class MultiCacheManager extends AbstractCacheManager {

    private final MultiCacheProperty multiCacheProperty;

    private final RedisTemplate<Object, Object> redisTemplate;

    private final boolean dynamic;

    public MultiCacheManager(MultiCacheProperty multiCacheProperty, RedisTemplate<Object, Object> redisTemplate) {
        this.multiCacheProperty = multiCacheProperty;
        this.redisTemplate = redisTemplate;
        this.dynamic = multiCacheProperty.isDynamic();
    }

    @Override
    protected Collection<? extends Cache> loadCaches() {
        List<MultiCache> caches = new LinkedList<>();
        for (String cacheName : multiCacheProperty.getCacheNames()) {
            caches.add(createCache(cacheName));
        }
        return caches;
    }

    @Override
    protected Cache getMissingCache(String name) {
        return dynamic ? createCache(name) : null;
    }

    private MultiCache createCache(String cacheName) {
        MultiCache newCache = new MultiCache(cacheName, redisTemplate, multiCacheProperty);
        log.debug("create cache instance, the cache name is : {}", cacheName);
        return newCache;
    }

    public void refreshCache(TopicMessage msg) {
        MultiCache cache = (MultiCache) getCache(msg.getCacheName());
        if (cache != null) {
            cache.pullMessage(msg);
        }
    }

}
