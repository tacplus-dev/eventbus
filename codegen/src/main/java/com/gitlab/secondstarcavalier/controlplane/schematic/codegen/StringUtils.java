// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.codegen;

/**
 * String utility methods for code generation naming conventions.
 */
public class StringUtils {

    /**
     * Converts snake_case to PascalCase.
     */
    public static String toPascalCase(String snake) {
        if (snake == null || snake.isEmpty()) return snake;
        StringBuilder sb = new StringBuilder();
        for (String part : snake.split("_")) {
            if (!part.isEmpty()) {
                sb.append(Character.toUpperCase(part.charAt(0)));
                if (part.length() > 1) sb.append(part.substring(1));
            }
        }
        return sb.toString();
    }

    /**
     * Converts snake_case to lowerCamelCase.
     */
    public static String toLowerCamelCase(String snake) {
        String pascal = toPascalCase(snake);
        if (pascal == null || pascal.isEmpty()) return pascal;
        return Character.toLowerCase(pascal.charAt(0)) + pascal.substring(1);
    }

    /**
     * Extracts simple type name from a potentially qualified type.
     * e.g., "aegis.TopologyEvent" -> "TopologyEvent"
     */
    public static String extractSimpleTypeName(String type) {
        if (type == null) return null;
        int dot = type.lastIndexOf('.');
        return dot >= 0 ? type.substring(dot + 1) : type;
    }
}
