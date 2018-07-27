package util;

import java.lang.reflect.ParameterizedType;

public class GenericClassUtils {
    public static <T> Class<T> getGenericClass(Class containerClass, int...indexes) {
        int index = indexes.length == 0 ? 0 : indexes[0];
        return (Class)((ParameterizedType) containerClass.getGenericSuperclass())
                .getActualTypeArguments()[index];
    }
}
