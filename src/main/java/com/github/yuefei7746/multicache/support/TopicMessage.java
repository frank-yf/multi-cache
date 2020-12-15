package com.github.yuefei7746.multicache.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.lang.Nullable;

import java.io.Serializable;

/**
 * @author yuefei7746
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicMessage implements Serializable {

    private static final long serialVersionUID = 5987219310442078193L;

    private String cacheName;

    @Nullable
    private Object key;

    @Nullable
    private Object value;

    public static TopicMessage create(String cacheName, @Nullable Object key) {
        return new TopicMessage(cacheName, key, null);
    }

    public static TopicMessage create(String cacheName, @Nullable Object key, Object value) {
        return new TopicMessage(cacheName, key, value);
    }

}
