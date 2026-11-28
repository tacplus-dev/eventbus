#!/usr/bin/env bash
# Copyright (c) 2026 Jody Scott
# SPDX-License-Identifier: Apache-2.0
#
# Every plugin the site lifecycle runs is pinned in the root pom's
# <pluginManagement> (#34). Unpinned, `mvn site` runs Maven's default
# maven-site-plugin (3.12.1 under Maven 3.9.16, dom4j 1.1: CRITICAL), and the
# site plugin adds maven-project-info-reports-plugin at whatever Maven Central
# has newest. Neither is in a build's own plugin list, so this reads the plugin
# class realms `mvn -X -N site` populates, and fails on any whose groupId:artifactId
# the root <pluginManagement> does not pin at the version that ran.
#
# Usage:  tools/check-site-plugins.sh [mvn args...]   (say -Dmaven.repo.local=...)
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
pom="$root/pom.xml"
log="$(mktemp)"
trap 'rm -f "$log"' EXIT

# -N: the root alone; the modules inherit its <pluginManagement> and add no <reporting>.
if ! mvn -B -X -N -f "$pom" "$@" site >"$log" 2>&1; then
  tail -n 40 "$log" >&2; echo "check-site-plugins: mvn site failed" >&2; exit 1
fi

# groupId:artifactId version of each plugin the root <pluginManagement> pins
# (the plugin's own coordinates, not its <dependencies>), ${property} resolved.
pinned="$(awk '
  /<pluginManagement>/ { pm = 1 } /<\/pluginManagement>/ { pm = 0 }
  !pm { next }
  /<plugin>/ { p = 1; d = 0; g = "org.apache.maven.plugins"; a = ""; v = "" }
  /<dependencies>/ { d = 1 } /<\/dependencies>/ { d = 0 }
  p && !d && /<groupId>/ { g = $0; gsub(/.*<groupId>|<\/groupId>.*/, "", g) }
  p && !d && /<artifactId>/ { a = $0; gsub(/.*<artifactId>|<\/artifactId>.*/, "", a) }
  p && !d && /<version>/ { v = $0; gsub(/.*<version>|<\/version>.*/, "", v) }
  /<\/plugin>/ { if (a != "") print g ":" a " " v; p = 0 }
' "$pom")"
resolve() {
  local v="$1" name
  while [[ "$v" =~ \$\{([^}]+)\} ]]; do
    name="${BASH_REMATCH[1]}"
    v="${v/\$\{$name\}/$(sed -n -E "s|.*<$name>([^<]*)</$name>.*|\1|p" "$pom" | head -1)}"
  done
  printf '%s' "$v"
}

ran="$(sed -n -E 's/.*Populating class realm plugin>([^:]+):([^:]+):([^[:space:]]+).*/\1:\2 \3/p' "$log" | sort -u)"
[[ -n "$ran" ]] || { echo "check-site-plugins: mvn site populated no plugin realm" >&2; exit 1; }

fail=0 ok=()
while read -r ga version; do
  want="$(awk -v ga="$ga" '$1 == ga { print $2; exit }' <<<"$pinned")"
  want="$(resolve "$want")"
  if [[ -z "$want" ]]; then
    echo "check-site-plugins: $ga $version runs in the site lifecycle but the root <pluginManagement> does not pin it" >&2; fail=1
  elif [[ "$want" != "$version" ]]; then
    echo "check-site-plugins: $ga ran at $version, but the root <pluginManagement> pins $want" >&2; fail=1
  else
    ok+=("$ga:$version")
  fi
done <<<"$ran"
(( fail == 0 )) || exit 1
echo "check-site-plugins: pinned: ${ok[*]}"
