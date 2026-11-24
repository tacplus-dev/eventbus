// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The release image builds only from bases pinned by digest, never a floating
 * tag (decisions/security-gates-ship-only-2026-11-23.md, point 2; #38): a
 * rebuild of the same commit ships the same, scanned, base. Nightly maintenance
 * moves the digests (tacplus-dev/looper#234).
 */
class DockerfilePinnedTest {

    // FROM [--platform=...] <image> [AS <stage>]
    private static final Pattern FROM = Pattern.compile(
            "^\\s*FROM\\s+(?:--\\S+\\s+)*(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PINNED = Pattern.compile("^[^@\\s]+:[^@\\s]+@sha256:[0-9a-f]{64}$");

    @Test
    void everyFrom_isPinnedByDigest() throws Exception {
        // Surefire runs in server/; the Dockerfile is at the repository's root.
        Path dockerfile = Path.of("..", "Dockerfile");
        assertTrue(Files.isRegularFile(dockerfile), "Dockerfile at " + dockerfile.toAbsolutePath());
        List<String> lines = Files.readAllLines(dockerfile);
        List<String> unpinned = new ArrayList<>();
        int froms = 0;
        for (int i = 0; i < lines.size(); i++) {
            Matcher m = FROM.matcher(lines.get(i));
            if (!m.find()) {
                continue;
            }
            froms++;
            if (!PINNED.matcher(m.group(1)).matches()) {
                unpinned.add("Dockerfile:" + (i + 1) + ": " + lines.get(i).strip());
            }
        }
        assertTrue(froms > 0, "no FROM in the Dockerfile");
        assertTrue(unpinned.isEmpty(),
                "FROM not pinned as <image>:<tag>@sha256:<digest>:\n" + String.join("\n", unpinned));
    }
}
