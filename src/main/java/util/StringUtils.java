package util;

public class StringUtils {

    public static String capitalize(String string, int charAt) {
        StringBuilder builder = new StringBuilder(string);
        char c = Character.toUpperCase(builder.charAt(charAt));
        builder.setCharAt(charAt, c);
        return builder.toString();
    }

    public static String removeAllChar(String string, char c) {
        StringBuilder builder = new StringBuilder(string);
        String character = String.valueOf(c);

        int pos = builder.indexOf(character);
        while (pos != -1) {
            builder.deleteCharAt(pos);
            pos = builder.indexOf(character);
        }

        return builder.toString();
    }

    public static String removeCharAt(String string, int charAt) {
        StringBuilder builder = new StringBuilder(string);

        builder.deleteCharAt(charAt);

        return builder.toString();
    }
}
