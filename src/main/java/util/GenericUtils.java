package util;

import java.lang.reflect.ParameterizedType;

public class GenericUtils {

    public static Class<?> getGenericType(Class clazz) {
        ParameterizedType stringListType = (ParameterizedType) clazz.getGenericSuperclass();
        return (Class<?>) stringListType.getActualTypeArguments()[0];
    }
}
