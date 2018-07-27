package util;

import com.eclipsesource.json.JsonValue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

class MinimalJsonTypeConverter {

    private static final Collection<Class> PRIMITIVE_TYPES = Arrays.asList(Boolean.class, Byte.class, Character.class,
            Short.class, Integer.class, Long.class, Float.class, Double.class);

    static boolean isPrimitive(Object object) {
        for (Class clazz : PRIMITIVE_TYPES) {
            Class c = object.getClass();
            if (clazz.equals(object.getClass())) {
                return true;
            }
        }
        return false;
    }

    static boolean isPrimitive(Class myClass) {
        for (Class clazz : PRIMITIVE_TYPES) {
            if (clazz.equals(myClass)) {
                return true;
            }
        }
        return false;
    }

    static boolean isString(Object object) {
        return String.class.equals(object.getClass());
    }

    static boolean isString(Class myClass) {
        return String.class.equals(myClass);
    }

    static boolean isList(Object object) {
        for (Class clazz : object.getClass().getInterfaces()) {
            if (clazz.equals(List.class)) {
                return true;
            }
        }
        return false;
    }

    static boolean isList(Class myClass) {
        for (Class clazz : myClass.getInterfaces()) {
            if (clazz.equals(List.class)) {
                return true;
            }
        }
        return false;
    }

    static boolean isMap(Object object) {
        for (Class clazz : object.getClass().getInterfaces()) {
            if (clazz.equals(Map.class)) {
                return true;
            }
        }
        return false;
    }

    static <T> T convertToObject(JsonValue jsonValue, Class<T> clazz) {
        Object object = null;

        if (jsonValue.isObject()) {
            object = jsonValue.asObject();

            // get json string
            if (String.class.equals(clazz)) {
                object = object.toString();
            }
        } else if (jsonValue.isString()) {
            object = jsonValue.asString();
        } else if (jsonValue.isBoolean()) {
            object = jsonValue.asBoolean();
        } else if (jsonValue.isArray()) {
            object = jsonValue.asArray();

            // convert JsonArray into List<JsonValue>
            if (List.class.equals(clazz)) {
                object = jsonValue.asArray().values()
                        .stream()
                        .map(MinimalJsonTypeConverter::convertToString);
            }
        } else if (jsonValue.isNumber()) {
            try {
                if (Integer.class.equals(clazz)) {
                    object = jsonValue.asInt();
                } else if (Long.class.equals(clazz)) {
                    object = jsonValue.asLong();
                } else if (Float.class.equals(clazz)) {
                    object = jsonValue.asFloat();
                } else if (Double.class.equals(clazz)) {
                    object = jsonValue.asDouble();
                }
            } catch (Exception ignore) {
            }
        }

        return (T) object;
    }

    public static String convertToString(JsonValue jsonValue) {
        if (jsonValue.isString()) {
            return jsonValue.asString();
        } else {
            return jsonValue.toString();
        }
    }
}
