package com.github.yuefei7746.multicache;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.yuefei7746.multicache.support.ClearLocalCacheListener;
import com.github.yuefei7746.multicache.support.MultiCacheManager;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * @author yuefei7746
 */
@RequiredArgsConstructor
@Configuration
@AutoConfigureAfter(RedisAutoConfiguration.class)
@EnableConfigurationProperties(MultiCacheProperty.class)
public class MultiCacheAutoConfiguration {

    private final MultiCacheProperty multiCacheProperty;

    private final RedisConnectionFactory redisConnectionFactory;

    @Bean
    @ConditionalOnMissingBean
    public MultiCacheManager cacheManager(RedisTemplate<Object, Object> redisTemplate) {
        return new MultiCacheManager(multiCacheProperty, redisTemplate);
    }

    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(MultiCacheManager cacheManager,
                                                                       RedisTemplate<Object, Object> redisTemplate) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory);
        container.addMessageListener(
                new ClearLocalCacheListener(redisTemplate, cacheManager),
                new ChannelTopic(multiCacheProperty.getRedis().getTopic()));
        container.afterPropertiesSet();
        return container;
    }

    @Bean
    @ConditionalOnMissingBean
    public RedisTemplate<Object, Object> redisTemplate(Jackson2JsonRedisSerializer<Object> jacksonSerializer) {
        RedisTemplate<Object, Object> redisTemplate = new RedisTemplate<>();

        redisTemplate.setDefaultSerializer(RedisSerializer.string());

        redisTemplate.setHashValueSerializer(jacksonSerializer);
        redisTemplate.setValueSerializer(jacksonSerializer);

        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    /**
     * Jackson 装配的 Redis 序列化器
     */
    @Bean
    public Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer() {
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.configure(MapperFeature.USE_ANNOTATIONS, false);
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        // 此项必须配置，否则会报java.lang.ClassCastException: java.util.LinkedHashMap cannot be cast to XXX
        om.activateDefaultTyping(om.getPolymorphicTypeValidator(),
                ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
        om.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer =
                new Jackson2JsonRedisSerializer<>(Object.class);
        jackson2JsonRedisSerializer.setObjectMapper(om);
        return jackson2JsonRedisSerializer;
    }

}
