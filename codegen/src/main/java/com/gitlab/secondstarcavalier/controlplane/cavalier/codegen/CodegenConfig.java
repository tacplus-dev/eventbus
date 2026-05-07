// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.codegen;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads codegen-config.json written by tacplus during code generation.
 */
public class CodegenConfig {

    public static String readJavaPackageBase(Path protoDir) throws IOException {
        Path configPath = protoDir.resolve("codegen-config.json");
        if (!Files.exists(configPath)) {
            // tacplus writes codegen-config.json under proto-annotated/
            configPath = protoDir.resolve("proto-annotated/codegen-config.json");
        }
        if (!Files.exists(configPath)) {
            return "";
        }
        String json = Files.readString(configPath);
        JsonObject obj = new Gson().fromJson(json, JsonObject.class);
        if (obj.has("javaPackageBase")) {
            return obj.get("javaPackageBase").getAsString();
        }
        return "";
    }
}
