package util;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import java.lang.reflect.Field;
import java.util.*;

import static util.MinimalJsonTypeConverter.convertToObject;

public class JsonUtils {

    public static final String EMPTY_OBJECT = "{}";
    private static final String EMPTY_LIST = "[]";
    private static final String NULL = "null";


    public static <T> List<T> getArray(String array, Class<T> clazz) {
        List<T> list = new LinkedList<>();
        try {
            JsonValue jsonValue = Json.parse(array);
            if (jsonValue.isArray()) {
                jsonValue.asArray().values().forEach(value -> list.add(convertToObject(value, clazz)));
            }
        } catch (Exception ignored) {
        }
        return list;
    }

    public static <T> T getValue(String jsonObject, String key, Class<T> clazz) {
        try {
            JsonObject object = Json.parse(jsonObject).asObject();
            JsonValue value = object.get(key);

            return convertToObject(value, clazz);
        } catch (Exception ignored) {
            return null;
        }
    }

    public static String getValue(String jsonObject, String key) {
        try {
            JsonObject object = Json.parse(jsonObject).asObject();
            JsonValue value = object.get(key);

            return MinimalJsonTypeConverter.convertToString(value);
        } catch (Exception ignored) {
            return null;
        }
    }

    public static Collection<String> getAllProperties(String object) {
        Collection<String> properties = new LinkedList<>();
        try {
            JsonObject jsonObject = Json.parse(object).asObject();
            jsonObject.forEach(member -> properties.add(member.getName()));
        } catch (Exception ignored) {
        }
        return properties;
    }

    public static <T> Map<String, T> toMap(String jsonObject, Class<T> clazz) {
        Map<String, T> map = new Hashtable<>();
        try {
            JsonObject object = Json.parse(jsonObject).asObject();
            object.forEach(member -> map.put(
                    member.getName(),
                    convertToObject(member.getValue(), clazz)
            ));
        } catch (Exception ignored) {
        }
        return map;
    }

    public static String toJson(Collection<String> keys, Collection<Object> values) {
        if (keys.size() != values.size()) return EMPTY_OBJECT;

        int size = keys.size();
        String[] arrKeys = keys.toArray(new String[size]);
        Object[] arrValues = values.toArray(new Object[size]);

        StringBuilder builder = new StringBuilder("{");
        for (int i = 0; i < size; i++) {
            builder.append("\"").append(arrKeys[i]).append("\"")
                    .append(":")
                    .append(toJson(arrValues[i]))
                    .append(",");
        }

        // delete the last comma at the end
        if (builder.lastIndexOf(",") != -1) {
            builder.deleteCharAt(builder.lastIndexOf(","));
        }

        builder.append("}");
        return builder.toString();
    }

    public static String toJson(Map<String, Object> object) {
        if (object == null) return EMPTY_OBJECT;

        StringBuilder builder = new StringBuilder("{");
        object.forEach((key, value) -> builder
                .append("\"").append(key).append("\"")
                .append(":")
                .append(toJson(value))
                .append(","));

        // delete the last comma at the end
        if (builder.lastIndexOf(",") != -1) {
            builder.deleteCharAt(builder.lastIndexOf(","));
        }

        builder.append("}");
        return builder.toString();
    }

    public static <T> String toJson(Collection<T> list) {
        if (list == null) return EMPTY_LIST;

        StringBuilder builder = new StringBuilder("[");
        for (T t : list) {
            builder.append(toJson(t)).append(",");
        }

        // delete the last comma at the end
        if (builder.lastIndexOf(",") != -1) {
            builder.deleteCharAt(builder.lastIndexOf(","));
        }

        builder.append("]");
        return builder.toString();
    }

    public static <T> String toJson(T object)  {
        if (object == null) {
            return NULL;
        }

        if (MinimalJsonTypeConverter.isPrimitive(object)) {
            return object.toString();
        }

        if (MinimalJsonTypeConverter.isString(object)) {
            return "\"" + object.toString() + "\"";
        }

        if (MinimalJsonTypeConverter.isList(object)) {
            return toJson((List) object);
        }

        Field[] fields = object.getClass().getDeclaredFields();
        StringBuilder builder = new StringBuilder("{");

        // append fields and values
        for (Field field : fields) {
            field.setAccessible(true);

            String key = field.getName();
            Object value;
            try {
                value = field.get(object);
            } catch (IllegalAccessException e) {
                value = "";
            }

            builder.append("\"").append(key).append("\"")
                    .append(":")
                    .append(toJson(value))
                    .append(",");
        }

        // delete the last comma at the end
        if (builder.lastIndexOf(",") != -1) {
            builder.deleteCharAt(builder.lastIndexOf(","));
        }

        builder.append("}");

        return builder.toString();
    }

    @Deprecated
    public static <T> T fromJson(String json, Class<T> clazz) {
        if (NULL.equals(json) || json == null) {
            return null;
        }

        if (MinimalJsonTypeConverter.isPrimitive(clazz)) {
            return convertToObject(Json.value(json), clazz);
        }

        if (MinimalJsonTypeConverter.isString(clazz)) {
            return (T) StringUtils.removeAllChar(json, '"');
        }

        if (MinimalJsonTypeConverter.isList(clazz)) {
            Class genericItemType = GenericUtils.getGenericType(clazz);
            return (T) getArray(json, genericItemType);
        }

        try {
            T object = clazz.getDeclaredConstructor().newInstance();

            Field[] fields = clazz.getDeclaredFields();
            JsonObject jsonObject = Json.parse(json).asObject();

            for (Field field : fields) {
                if (jsonObject.get(field.getName()) != null) {
                    String value = convertToObject(jsonObject.get(field.getName()), String.class);

                    field.setAccessible(true);
                    field.set(object, fromJson(value, field.getType()));
                }
            }

            return object;
        } catch (Exception e) {
            return null;
        }
    }
}
