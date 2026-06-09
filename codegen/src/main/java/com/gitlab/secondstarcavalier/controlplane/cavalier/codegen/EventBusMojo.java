// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.codegen;

import com.gitlab.secondstarcavalier.controlplane.cavalier.codegen.generator.*;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * Maven mojo that generates typed Publish/Subscribe gRPC RPCs for Kafka topics.
 * Reads registry.json (produced by tacplus) to find state roots backed by
 * eventbus with topic-annotated fields, then generates typed streaming RPCs.
 */
@Mojo(name = "generate-eventbus-rpcs", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class EventBusMojo extends AbstractMojo {

    @Parameter(property = "registryFile",
               defaultValue = "${project.build.directory}/generated-sources/model/registry.json")
    private File registryFile;

    @Parameter(property = "outputDir",
               defaultValue = "${project.build.directory}/generated-sources/model")
    private File outputDir;

    @Parameter(property = "javaPackageBase")
    private String javaPackageBase;

    @Override
    public void execute() throws MojoExecutionException {
        getLog().info("EventBus codegen: reading registry from " + registryFile);

        if (!registryFile.exists()) {
            getLog().warn("Registry file does not exist: " + registryFile);
            return;
        }

        try {
            // Read codegen-config.json for package base if not set
            if (javaPackageBase == null || javaPackageBase.isEmpty()) {
                Path protoDir = registryFile.toPath().getParent();
                javaPackageBase = CodegenConfig.readJavaPackageBase(protoDir);
            }

            // Discover topics from registry.json
            TopicDiscovery discovery = new TopicDiscovery(registryFile.toPath());
            List<TopicDiscovery.TopicField> topics = discovery.discoverTopics();

            getLog().info("Discovered " + topics.size() + " eventbus topics");

            if (topics.isEmpty()) {
                getLog().info("No eventbus topics found, skipping code generation");
                return;
            }

            // Generate typed RPC bindings
            Path outPath = outputDir.toPath();
            TypedRpcGenerator rpcGen = new TypedRpcGenerator(outPath, javaPackageBase);
            rpcGen.generate(topics);

            // Generate topic registry
            TopicRegistryGenerator registryGen = new TopicRegistryGenerator(outPath, javaPackageBase);
            registryGen.generate(topics);

            getLog().info("Generated typed RPCs for " + topics.size() + " topics");

        } catch (IOException e) {
            throw new MojoExecutionException("EventBus codegen failed", e);
        }
    }
}
