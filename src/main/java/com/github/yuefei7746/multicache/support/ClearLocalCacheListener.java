package com.github.yuefei7746.multicache.support;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.Nullable;

/**
 * @author yuefei7746
 */
@Slf4j
@RequiredArgsConstructor
public class ClearLocalCacheListener implements MessageListener {

    private final RedisTemplate<Object, Object> redisTemplate;

    private final MultiCacheManager multiCacheManager;

    @Override
    public void onMessage(Message message, @Nullable byte[] pattern) {
        TopicMessage msg = (TopicMessage) redisTemplate.getValueSerializer().deserialize(message.getBody());
        if (msg != null) {
            log.debug("receive a redis topic message, clear local cache, the cacheName is {}, the key is {}",
                    msg.getCacheName(), msg.getKey());
            multiCacheManager.refreshCache(msg);
        }
    }

}
