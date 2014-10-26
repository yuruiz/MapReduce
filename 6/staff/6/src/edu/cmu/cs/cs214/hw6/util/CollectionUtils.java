package edu.cmu.cs.cs214.hw6.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is not part of the student starter code.
 */
public final class CollectionUtils {

    @SafeVarargs
    public static <T> List<T> asList(T... elems) {
        List<T> list = new ArrayList<>();
        for (T elem : elems) {
            list.add(elem);
        }
        return list;
    }

    public static <K, V> void putIfAbsent(Map<K, List<V>> map, K key, V value) {
        if (map.containsKey(key)) {
            map.get(key).add(value);
        } else {
            map.put(key, asList(value));
        }
    }

    private CollectionUtils() {
    }

}
