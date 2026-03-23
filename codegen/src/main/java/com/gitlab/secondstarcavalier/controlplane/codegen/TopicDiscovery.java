// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.codegen;

import com.google.gson.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Discovers topic fields from registry.json.
 * Finds BoundaryInfo entries where store == "eventbus" and graph_type == "topic",
 * then collects their metadata for typed RPC generation.
 */
public class TopicDiscovery {

    private final Path registryPath;

    public TopicDiscovery(Path registryPath) {
        this.registryPath = registryPath;
    }

    /**
     * A discovered topic field with its context.
     */
    public static class TopicField {
        private final String stateName;     // e.g. "AegisState"
        private final String statePackage;  // e.g. "aegis"
        private final String fieldName;     // e.g. "event_stream"
        private final String fieldType;     // FQN e.g. "aegis.TopologyEvent"
        private final String topicName;     // Kafka topic name
        private final String packagePath;   // e.g. "services/aegis"

        public TopicField(String stateName, String statePackage, String fieldName,
                          String fieldType, String topicName, String packagePath) {
            this.stateName = stateName;
            this.statePackage = statePackage;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.topicName = topicName;
            this.packagePath = packagePath;
        }

        public String getStateName() { return stateName; }
        public String getStatePackage() { return statePackage; }
        public String getFieldName() { return fieldName; }
        public String getFieldType() { return fieldType; }
        public String getTopicName() { return topicName; }
        public String getPackagePath() { return packagePath; }

        public String getFieldPascal() {
            return StringUtils.toPascalCase(fieldName);
        }

        /**
         * Returns the fully qualified Java outer class import for the topic's proto type.
         * Proto messages are nested inside the outer class (e.g., Aegis.TopologyEvent).
         */
        public String getJavaOuterClassImport(String javaPackageBase) {
            // fieldType is FQN like "aegis.TopologyEvent"
            // Outer class: {base}.types.services.{protoPackage}.{ProtoPackagePascal}
            String protoPackage = fieldType.contains(".")
                    ? fieldType.substring(0, fieldType.lastIndexOf('.'))
                    : statePackage;
            String outerClass = StringUtils.toPascalCase(protoPackage);
            return javaPackageBase + ".types.services." + protoPackage + "." + outerClass;
        }

        /**
         * Returns the qualified Java type reference for use in generated code.
         * E.g., "Aegis.TopologyEvent" (outer class dot nested message).
         */
        public String getJavaQualifiedType() {
            String simpleType = StringUtils.extractSimpleTypeName(fieldType);
            String protoPackage = fieldType.contains(".")
                    ? fieldType.substring(0, fieldType.lastIndexOf('.'))
                    : statePackage;
            String outerClass = StringUtils.toPascalCase(protoPackage);
            return outerClass + "." + simpleType;
        }
    }

    /**
     * Reads registry.json and discovers eventbus-backed topics.
     * Walks the PackageNode tree looking for BoundaryInfo entries with
     * store == "eventbus" and graph_type == "topic".
     */
    public List<TopicField> discoverTopics() throws IOException {
        String json = Files.readString(registryPath);
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();

        List<TopicField> topics = new ArrayList<>();
        walkNode(root, topics);
        return topics;
    }

    private void walkNode(JsonObject node, List<TopicField> topics) {
        String packagePath = node.has("full_path")
                ? node.get("full_path").getAsString() : "";

        // Check boundaries in this node (existing eventbus topic discovery).
        if (node.has("boundaries")) {
            JsonArray boundaries = node.getAsJsonArray("boundaries");

            for (JsonElement elem : boundaries) {
                JsonObject boundary = elem.getAsJsonObject();

                String store = getStr(boundary, "store");
                String graphType = getStr(boundary, "graph_type");

                if ("eventbus".equals(store) && "topic".equals(graphType)) {
                    String stateRoot = getStr(boundary, "state_root");
                    String fieldName = getStr(boundary, "boundary");
                    String typeFqn = getStr(boundary, "type_fqn");

                    // Derive package from the type FQN.
                    String protoPackage = typeFqn.contains(".")
                            ? typeFqn.substring(0, typeFqn.lastIndexOf('.'))
                            : "";

                    // Build Kafka topic name: {packagePath}.{stateRoot}.{FieldPascal}
                    String fieldPascal = StringUtils.toPascalCase(fieldName);
                    String topicName = packagePath.replace('/', '.') + "." +
                            protoPackage + "." + stateRoot + "." + fieldPascal;

                    topics.add(new TopicField(
                            stateRoot, protoPackage, fieldName,
                            typeFqn, topicName, packagePath));
                }
            }
        }

        // Check types for bigdata-annotated messages (BigData event model).
        if (node.has("types")) {
            JsonArray types = node.getAsJsonArray("types");
            for (JsonElement elem : types) {
                JsonObject type = elem.getAsJsonObject();
                String bigdataName = getStr(type, "bigdata_name");
                if (!bigdataName.isEmpty()) {
                    String fqn = getStr(type, "fqn");
                    String pkg = getStr(type, "package");
                    String topicName = "bigdata." + bigdataName;
                    // stateName is empty for bigdata types — rpcPrefix uses fieldName only.
                    topics.add(new TopicField(
                            "", pkg, bigdataName,
                            fqn, topicName, packagePath));
                }
            }
        }

        // Recurse into children.
        if (node.has("children")) {
            JsonArray children = node.getAsJsonArray("children");
            for (JsonElement child : children) {
                walkNode(child.getAsJsonObject(), topics);
            }
        }
    }

    private static String getStr(JsonObject obj, String key) {
        return obj.has(key) ? obj.get(key).getAsString() : "";
    }
}
