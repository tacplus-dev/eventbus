// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus.codegen;

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
        // The package of the proto that DECLARES the state message, which is not
        // always statePackage: statePackage is derived from the event TYPE's FQN.
        // For agentsensor.AgentEventBus.event_stream the event type is
        // agentsensor.AgentEvent and the two agree; for TraceEventBus the event
        // type is trace.TraceEvent while the message is declared in agentsensor,
        // and they diverge. Only this value matches what the generated proto
        // declares, so only this may be used to build a per-topic service name.
        // statePackage still composes the Kafka topic name and must not move.
        private final String declaringPackage;
        private final String fieldName;     // e.g. "event_stream"
        private final String fieldType;     // FQN e.g. "aegis.TopologyEvent"
        private final String topicName;     // Kafka topic name
        private final String packagePath;   // e.g. "services/aegis"
        private final String typePathPrefix; // e.g. "services" or "common"
        private final boolean hasSpiffeIdField; // true if the type has a string spiffe_id field (C2S topics)
        private final boolean hasActorSpiffeIdField; // true if the type has a string actor_spiffe_id field (item 6 Piece A)
        private final boolean hasNamespaceField; // true if the type has a string namespace field (the (spec.store_namespace) tenant boundary)
        private String javaOuterClass;      // resolved from the registry; see setJavaOuterClass
        // rpc name -> auth_services patterns, read from the registry entry for this
        // topic's generated <Topic>StateService. Resolved in discoverTopics().
        private Map<String, List<String>> methodAuthServices = new LinkedHashMap<>();

        public TopicField(String stateName, String statePackage, String fieldName,
                          String fieldType, String topicName, String packagePath) {
            this(stateName, statePackage, fieldName, fieldType, topicName, packagePath, "services", false, statePackage);
        }

        public TopicField(String stateName, String statePackage, String fieldName,
                          String fieldType, String topicName, String packagePath,
                          String typePathPrefix) {
            this(stateName, statePackage, fieldName, fieldType, topicName, packagePath, typePathPrefix, false, statePackage);
        }

        public TopicField(String stateName, String statePackage, String fieldName,
                          String fieldType, String topicName, String packagePath,
                          String typePathPrefix, boolean hasSpiffeIdField) {
            this(stateName, statePackage, fieldName, fieldType, topicName, packagePath,
                    typePathPrefix, hasSpiffeIdField, statePackage);
        }

        public TopicField(String stateName, String statePackage, String fieldName,
                          String fieldType, String topicName, String packagePath,
                          String typePathPrefix, boolean hasSpiffeIdField,
                          String declaringPackage) {
            this(stateName, statePackage, fieldName, fieldType, topicName, packagePath,
                    typePathPrefix, hasSpiffeIdField, declaringPackage, false);
        }

        public TopicField(String stateName, String statePackage, String fieldName,
                          String fieldType, String topicName, String packagePath,
                          String typePathPrefix, boolean hasSpiffeIdField,
                          String declaringPackage, boolean hasActorSpiffeIdField) {
            this(stateName, statePackage, fieldName, fieldType, topicName, packagePath,
                    typePathPrefix, hasSpiffeIdField, declaringPackage, hasActorSpiffeIdField, false);
        }

        public TopicField(String stateName, String statePackage, String fieldName,
                          String fieldType, String topicName, String packagePath,
                          String typePathPrefix, boolean hasSpiffeIdField,
                          String declaringPackage, boolean hasActorSpiffeIdField,
                          boolean hasNamespaceField) {
            this.declaringPackage = declaringPackage == null || declaringPackage.isEmpty()
                    ? statePackage : declaringPackage;
            this.stateName = stateName;
            this.statePackage = statePackage;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.topicName = topicName;
            this.packagePath = packagePath;
            this.typePathPrefix = typePathPrefix;
            this.hasSpiffeIdField = hasSpiffeIdField;
            this.hasActorSpiffeIdField = hasActorSpiffeIdField;
            this.hasNamespaceField = hasNamespaceField;
        }

        public String getStateName() { return stateName; }
        public String getStatePackage() { return statePackage; }

        /**
         * The package of the proto declaring the state message — the package the
         * generated proto puts the per-topic {@code <Topic>StateService} in.
         */
        public String getDeclaringPackage() { return declaringPackage; }
        /**
         * The per-topic gRPC service name, matching what tacplus declares in the
         * generated proto: {@code <declaringPackage>.<StateName>StateService}.
         * getDeclaringPackage, NOT getStatePackage — the latter is the event
         * TYPE's package and produces "trace.TraceEventBusStateService", a name
         * no proto declares.
         */
        public String getPerTopicServiceName() {
            return declaringPackage + "." + stateName + "StateService";
        }

        /** The topic's publish rpc name, as the generated proto declares it. */
        public String getPublishRpcName() { return "PublishTo" + getFieldPascal(); }

        /** The topic's subscribe rpc name, as the generated proto declares it. */
        public String getSubscribeRpcName() { return "SubscribeTo" + getFieldPascal(); }

        public void setMethodAuthServices(Map<String, List<String>> m) { this.methodAuthServices = m; }

        /**
         * rpc name -> the auth_services patterns declared on that rpc by
         * (spec.service_s2s) in the generated proto. This is the policy the
         * chassis Authorizer enforces for the topic; empty means the registry
         * declared none.
         */
        public Map<String, List<String>> getMethodAuthServices() { return methodAuthServices; }

        public String getFieldName() { return fieldName; }
        public String getFieldType() { return fieldType; }
        public String getTopicName() { return topicName; }
        public String getPackagePath() { return packagePath; }
        public boolean hasSpiffeIdField() { return hasSpiffeIdField; }
        public boolean hasActorSpiffeIdField() { return hasActorSpiffeIdField; }
        public boolean hasNamespaceField() { return hasNamespaceField; }


        public String getFieldPascal() {
            return StringUtils.toPascalCase(fieldName);
        }

        /** The proto package the topic's event TYPE lives in. */
        private String typeProtoPackage() {
            return fieldType.contains(".")
                    ? fieldType.substring(0, fieldType.lastIndexOf('.'))
                    : statePackage;
        }

        /**
         * The outer class protoc puts the package's messages in. Normally the
         * pascal-cased package, but protoc appends "OuterClass" when a top-level
         * message, enum or service in the file already has that name — as
         * tasks.proto's {@code service Tasks} does. Resolved from the registry in
         * {@link #discoverTopics()}; the unresolved fallback is the plain form.
         */
        void setJavaOuterClass(String javaOuterClass) {
            this.javaOuterClass = javaOuterClass;
        }

        private String javaOuterClass() {
            return javaOuterClass != null && !javaOuterClass.isEmpty()
                    ? javaOuterClass
                    : StringUtils.toPascalCase(typeProtoPackage());
        }

        /**
         * Returns the fully qualified Java outer class import for the topic's proto type.
         * Proto messages are nested inside the outer class (e.g., Aegis.TopologyEvent).
         * Handles types from both services/ and common/ proto directories.
         */
        public String getJavaOuterClassImport(String javaPackageBase) {
            return javaPackageBase + ".types." + typePathPrefix + "." + typeProtoPackage()
                    + "." + javaOuterClass();
        }

        /**
         * Returns the fully qualified generated Helpers class for the topic's proto
         * package. tacplus emits one Helpers per proto package, carrying a
         * validate&lt;Type&gt;(msg) for every message in it — which is what the publish
         * path uses to reject an event that violates its own declared constraints.
         */
        public String getHelpersClassImport(String javaPackageBase) {
            String protoPackage = fieldType.contains(".")
                    ? fieldType.substring(0, fieldType.lastIndexOf('.'))
                    : statePackage;
            return javaPackageBase + ".types." + typePathPrefix + "." + protoPackage + ".Helpers";
        }

        /**
         * Returns the qualified Java type reference for use in generated code.
         * E.g., "Aegis.TopologyEvent" (outer class dot nested message).
         */
        public String getJavaQualifiedType() {
            String simpleType = StringUtils.extractSimpleTypeName(fieldType);
            return javaOuterClass() + "." + simpleType;
        }
    }

    /**
     * Reads registry.json and discovers eventbus-backed topics.
     * Walks the PackageNode tree looking for BoundaryInfo entries with
     * tag == "eventbus" and graph_type == "topic".
     */
    public List<TopicField> discoverTopics() throws IOException {
        String json = Files.readString(registryPath);
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();

        // Build a map of proto package -> path prefix (e.g., "sensor" -> "common",
        // "aegis" -> "services") by walking the tree. This allows us to resolve
        // whether a type lives under common/ or services/ for Java imports.
        Map<String, String> packagePathMap = new HashMap<>();
        collectPackagePaths(root, packagePathMap);

        // Build a set of type FQNs that have a "string spiffe_id" field.
        // Used to determine which topics need identity stamping (C2S topics).
        Set<String> typesWithSpiffeId = new HashSet<>();
        collectTypesWithSpiffeIdField(root, typesWithSpiffeId);

        // Build a set of type FQNs that have a "string actor_spiffe_id" field
        // (item 6 Piece A) — the OPTIONAL delegated-mint actor provenance
        // field, distinct from and independent of spiffe_id above.
        Set<String> typesWithActorSpiffeId = new HashSet<>();
        collectTypesWithActorSpiffeIdField(root, typesWithActorSpiffeId);

        // Build a set of type FQNs that have a "string namespace" field — the
        // (spec.store_namespace) tenant boundary every (spec.record) must now
        // declare. Matched by literal field name for the same reason spiffe_id
        // is: registry.json carries no store_namespace marker (llmdesc's
        // FieldDescriptor has no such key), so the annotation itself is not
        // readable here. All five fleet records spell the field "namespace".
        Set<String> typesWithNamespace = new HashSet<>();
        collectTypesWithNamespaceField(root, typesWithNamespace);

        // Every top-level name protoc sees per proto package. A package whose
        // pascal-cased name is taken by one of them gets the "OuterClass" suffix.
        Map<String, Set<String>> topLevelNames = new HashMap<>();
        collectTopLevelNames(root, topLevelNames);

        // grpc_path -> auth_services, the per-rpc S2S policy tacplus computes from
        // (spec.service_s2s). The per-topic state services are hosted here but
        // their wrappers are generated for their owning service, so this registry
        // entry is the only place their policy is readable from.
        Map<String, List<String>> rpcAuthServices = new HashMap<>();
        collectRpcAuthServices(root, rpcAuthServices);

        List<TopicField> topics = new ArrayList<>();
        walkNode(root, topics, packagePathMap, typesWithSpiffeId, typesWithActorSpiffeId,
                typesWithNamespace);
        for (TopicField topic : topics) {
            topic.setJavaOuterClass(resolveOuterClass(topic.typeProtoPackage(), topLevelNames));
            topic.setMethodAuthServices(resolveMethodAuth(topic, rpcAuthServices));
        }
        return topics;
    }


    /**
     * Resolves the auth_services policy for a topic's two rpcs by their grpc_path
     * in the registry. A topic whose rpcs carry no policy yields an empty map;
     * the caller decides what that means.
     */
    private Map<String, List<String>> resolveMethodAuth(TopicField topic,
                                                        Map<String, List<String>> rpcAuthServices) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        String svc = topic.getPerTopicServiceName();
        for (String rpc : new String[] {topic.getPublishRpcName(), topic.getSubscribeRpcName()}) {
            List<String> patterns = rpcAuthServices.get("/" + svc + "/" + rpc);
            if (patterns != null && !patterns.isEmpty()) {
                result.put(rpc, patterns);
            }
        }
        return result;
    }

    /**
     * Walks the registry tree collecting every rpc's auth_services keyed by its
     * grpc_path ("/pkg.Service/Rpc").
     */
    private void collectRpcAuthServices(JsonObject node, Map<String, List<String>> result) {
        collectRpcAuthServicesFrom(node, result);
        if (node.has("services")) {
            for (JsonElement elem : node.getAsJsonArray("services")) {
                collectRpcAuthServicesFrom(elem.getAsJsonObject(), result);
            }
        }
        if (node.has("children")) {
            for (JsonElement child : node.getAsJsonArray("children")) {
                collectRpcAuthServices(child.getAsJsonObject(), result);
            }
        }
    }

    private void collectRpcAuthServicesFrom(JsonObject node, Map<String, List<String>> result) {
        if (!node.has("rpcs")) {
            return;
        }
        for (JsonElement elem : node.getAsJsonArray("rpcs")) {
            JsonObject rpc = elem.getAsJsonObject();
            String grpcPath = getStr(rpc, "grpc_path");
            if (grpcPath.isEmpty() || !rpc.has("auth_services")) {
                continue;
            }
            List<String> patterns = new ArrayList<>();
            for (JsonElement p : rpc.getAsJsonArray("auth_services")) {
                patterns.add(p.getAsString());
            }
            if (!patterns.isEmpty()) {
                result.put(grpcPath, patterns);
            }
        }
    }

    /**
     * Mirrors protoc's outer class naming: the pascal-cased proto package, plus
     * an "OuterClass" suffix when a top-level message, enum or service in that
     * package already carries the name. tacplus emits the same suffix into
     * java_outer_classname, so deriving without it produces an import of a class
     * that was never generated.
     */
    private String resolveOuterClass(String protoPackage, Map<String, Set<String>> topLevelNames) {
        String derived = StringUtils.toPascalCase(protoPackage);
        Set<String> names = topLevelNames.get(protoPackage);
        return names != null && names.contains(derived) ? derived + "OuterClass" : derived;
    }

    /**
     * Walks the registry tree collecting, per proto package, the simple names of
     * every top-level message, enum and service it declares.
     */
    private void collectTopLevelNames(JsonObject node, Map<String, Set<String>> result) {
        for (String key : new String[] {"types", "enums", "services"}) {
            if (!node.has(key)) {
                continue;
            }
            for (JsonElement elem : node.getAsJsonArray(key)) {
                JsonObject decl = elem.getAsJsonObject();
                String pkg = getStr(decl, "package");
                String name = getStr(decl, "name");
                if (name.isEmpty()) {
                    name = StringUtils.extractSimpleTypeName(getStr(decl, "fqn"));
                }
                if (!pkg.isEmpty() && !name.isEmpty()) {
                    result.computeIfAbsent(pkg, k -> new HashSet<>()).add(name);
                }
            }
        }
        if (node.has("children")) {
            for (JsonElement child : node.getAsJsonArray("children")) {
                collectTopLevelNames(child.getAsJsonObject(), result);
            }
        }
    }

    /**
     * Walks the registry tree and maps proto package names to their path prefix.
     * E.g., a node with full_path "common/sensor" maps package "sensor" -> "common".
     */
    private void collectPackagePaths(JsonObject node, Map<String, String> map) {
        String fullPath = getStr(node, "full_path");
        String name = getStr(node, "name");
        if (!fullPath.isEmpty() && !name.isEmpty() && fullPath.contains("/")) {
            // Extract prefix: "services/aegis" -> "services", "common/sensor" -> "common"
            String prefix = fullPath.substring(0, fullPath.indexOf('/'));
            map.put(name, prefix);
            // Also map proto package names from types (may differ from directory name,
            // e.g. directory "claude-sensor" has proto package "claudesensor").
            // When a package appears in both services/ and vservices/, prefer vservices/
            // since event types for topics always come from vservices.
            if (node.has("types")) {
                for (JsonElement elem : node.getAsJsonArray("types")) {
                    String pkg = getStr(elem.getAsJsonObject(), "package");
                    if (!pkg.isEmpty()) {
                        String existing = map.get(pkg);
                        if (existing == null || "services".equals(existing)) {
                            map.put(pkg, prefix);
                        }
                    }
                }
            }
        }
        if (node.has("children")) {
            for (JsonElement child : node.getAsJsonArray("children")) {
                collectPackagePaths(child.getAsJsonObject(), map);
            }
        }
    }

    /**
     * Walks the registry tree collecting FQNs of types that have a "string spiffe_id" field.
     */
    private void collectTypesWithSpiffeIdField(JsonObject node, Set<String> result) {
        if (node.has("types")) {
            for (JsonElement elem : node.getAsJsonArray("types")) {
                JsonObject type = elem.getAsJsonObject();
                String fqn = getStr(type, "fqn");
                if (!fqn.isEmpty() && type.has("fields")) {
                    for (JsonElement fieldElem : type.getAsJsonArray("fields")) {
                        JsonObject field = fieldElem.getAsJsonObject();
                        if ("spiffe_id".equals(getStr(field, "name"))
                                && "TYPE_KIND_STRING".equals(getStr(field, "type"))) {
                            result.add(fqn);
                            break;
                        }
                    }
                }
            }
        }
        if (node.has("children")) {
            for (JsonElement child : node.getAsJsonArray("children")) {
                collectTypesWithSpiffeIdField(child.getAsJsonObject(), result);
            }
        }
    }

    /**
     * Walks the registry tree collecting FQNs of types that have a "string
     * actor_spiffe_id" field (item 6 Piece A) — mirrors
     * {@link #collectTypesWithSpiffeIdField} exactly, by literal field name,
     * for the OPTIONAL delegated-mint actor provenance field.
     */
    private void collectTypesWithActorSpiffeIdField(JsonObject node, Set<String> result) {
        if (node.has("types")) {
            for (JsonElement elem : node.getAsJsonArray("types")) {
                JsonObject type = elem.getAsJsonObject();
                String fqn = getStr(type, "fqn");
                if (!fqn.isEmpty() && type.has("fields")) {
                    for (JsonElement fieldElem : type.getAsJsonArray("fields")) {
                        JsonObject field = fieldElem.getAsJsonObject();
                        if ("actor_spiffe_id".equals(getStr(field, "name"))
                                && "TYPE_KIND_STRING".equals(getStr(field, "type"))) {
                            result.add(fqn);
                            break;
                        }
                    }
                }
            }
        }
        if (node.has("children")) {
            for (JsonElement child : node.getAsJsonArray("children")) {
                collectTypesWithActorSpiffeIdField(child.getAsJsonObject(), result);
            }
        }
    }


    /**
     * Walks the registry tree collecting FQNs of types that have a "string
     * namespace" field — the (spec.store_namespace) tenant boundary. Mirrors
     * {@link #collectTypesWithSpiffeIdField} exactly, by literal field name.
     */
    private void collectTypesWithNamespaceField(JsonObject node, Set<String> result) {
        if (node.has("types")) {
            for (JsonElement elem : node.getAsJsonArray("types")) {
                JsonObject type = elem.getAsJsonObject();
                String fqn = getStr(type, "fqn");
                if (!fqn.isEmpty() && type.has("fields")) {
                    for (JsonElement fieldElem : type.getAsJsonArray("fields")) {
                        JsonObject field = fieldElem.getAsJsonObject();
                        if ("namespace".equals(getStr(field, "name"))
                                && "TYPE_KIND_STRING".equals(getStr(field, "type"))) {
                            result.add(fqn);
                            break;
                        }
                    }
                }
            }
        }
        if (node.has("children")) {
            for (JsonElement child : node.getAsJsonArray("children")) {
                collectTypesWithNamespaceField(child.getAsJsonObject(), result);
            }
        }
    }


    /**
     * The proto package declared by this registry node, e.g. the
     * vservices/agent-sensor node declares package "agentsensor". Falls back to
     * the supplied default when the node carries no types.
     */
    private String nodePackage(JsonObject node, String fallback) {
        if (node.has("types")) {
            for (JsonElement elem : node.getAsJsonArray("types")) {
                String pkg = getStr(elem.getAsJsonObject(), "package");
                if (!pkg.isEmpty()) {
                    return pkg;
                }
            }
        }
        return fallback;
    }

    private void walkNode(JsonObject node, List<TopicField> topics,
                          Map<String, String> packagePathMap, Set<String> typesWithSpiffeId,
                          Set<String> typesWithActorSpiffeId, Set<String> typesWithNamespace) {
        String packagePath = node.has("full_path")
                ? node.get("full_path").getAsString() : "";

        // Check boundaries in this node (eventbus topic discovery).
        if (node.has("boundaries")) {
            JsonArray boundaries = node.getAsJsonArray("boundaries");

            for (JsonElement elem : boundaries) {
                JsonObject boundary = elem.getAsJsonObject();

                String graphType = getStr(boundary, "graph_type");

                if (hasLabel(boundary, "eventbus") && "topic".equals(graphType)) {
                    String stateRoot = getStr(boundary, "state_root");
                    String fieldName = getStr(boundary, "boundary");
                    String typeFqn = getStr(boundary, "type_fqn");

                    // Derive package from the type FQN.
                    String protoPackage = typeFqn.contains(".")
                            ? typeFqn.substring(0, typeFqn.lastIndexOf('.'))
                            : "";

                    // Resolve whether the type lives under services/ or common/.
                    String typePrefix = packagePathMap.getOrDefault(protoPackage, "services");

                    // Build Kafka topic name: {packagePath}.{stateRoot}.{FieldPascal}
                    String fieldPascal = StringUtils.toPascalCase(fieldName);
                    String topicName = packagePath.replace('/', '.') + "." +
                            protoPackage + "." + stateRoot + "." + fieldPascal;

                    // The package that DECLARES this state message, read from the
                    // node the boundary was found on. protoPackage above is the
                    // event TYPE's package and is not the same thing whenever a
                    // topic carries a type from another proto — which is exactly
                    // the trace topic. Only this is safe to name a service with.
                    String declaringPackage = nodePackage(node, protoPackage);

                    topics.add(new TopicField(
                            stateRoot, protoPackage, fieldName,
                            typeFqn, topicName, packagePath, typePrefix,
                            typesWithSpiffeId.contains(typeFqn), declaringPackage,
                            typesWithActorSpiffeId.contains(typeFqn),
                            typesWithNamespace.contains(typeFqn)));
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
                    String typePrefix = packagePathMap.getOrDefault(pkg, "services");
                    topics.add(new TopicField(
                            "", pkg, bigdataName,
                            fqn, topicName, packagePath, typePrefix,
                            typesWithSpiffeId.contains(fqn)));
                }
            }
        }

        // Recurse into children.
        if (node.has("children")) {
            JsonArray children = node.getAsJsonArray("children");
            for (JsonElement child : children) {
                walkNode(child.getAsJsonObject(), topics, packagePathMap, typesWithSpiffeId,
                        typesWithActorSpiffeId, typesWithNamespace);
            }
        }
    }


    private static String getStr(JsonObject obj, String key) {
        return obj.has(key) ? obj.get(key).getAsString() : "";
    }

    /** True when the boundary carries the given routing label. tacplus emits
     *  these under the repeated "label" key (formerly the scalar "tag"). */
    private static boolean hasLabel(JsonObject boundary, String label) {
        if (!boundary.has("label") || !boundary.get("label").isJsonArray()) {
            return false;
        }
        for (JsonElement elem : boundary.getAsJsonArray("label")) {
            if (label.equals(elem.getAsString())) {
                return true;
            }
        }
        return false;
    }
}
