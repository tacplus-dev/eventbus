#!/usr/bin/env bash
# Copyright (c) 2026 Jody Scott
# SPDX-License-Identifier: Apache-2.0
#
# The release rules (tools/release-tags.sh, tools/release-notes.sh), run
# against fixture repositories: which image tags a version gets, so a fleet's
# MAJOR.MINOR (tacplus-dev/tacplus#180) only ever moves forward on its own
# line, and which commits its notes list. Part of `just gates`.
#
# Usage:  tools/release_test.sh
set -euo pipefail

tools="$(cd "$(dirname "$0")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
failed=0

g() { git -c user.name=t -c user.email=t@t -c tag.gpgSign=false -c commit.gpgSign=false "$@"; }

# A repository with one commit per tag, tagged in push order; each commit's
# subject is "change for <tag>".
fixture() { # name tag...
  local dir="$work/$1" t
  shift
  mkdir -p "$dir"
  g -C "$dir" init -q -b main
  g -C "$dir" commit -q --allow-empty -m first
  for t in "$@"; do
    g -C "$dir" commit -q --allow-empty -m "change for $t"
    g -C "$dir" tag "$t"
  done
  echo "$dir"
}

check() { # what got want
  if [[ "$2" == "$3" ]]; then
    echo "ok   $1"
  else
    printf 'FAIL %s\n  got:  %q\n  want: %q\n' "$1" "$2" "$3"
    failed=1
  fi
}

tags() { # name version tag...
  local name="$1" version="$2" dir
  shift 2
  dir="$(fixture "$name" "$@")"
  (cd "$dir" && "$tools/release-tags.sh" "$version")
}

notes() { # dir version
  (cd "$1" && "$tools/release-notes.sh" "$2")
}

check "the first release takes its line" "$(tags first v0.1.0 v0.1.0)" "0.1.0 0.1"
# The issue's acceptance (#2): v0.1.1 moves 0.1, and not an older line.
check "a patch moves its own line and no other" "$(tags patch v0.1.1 v0.0.3 v0.1.0 v0.1.1)" "0.1.1 0.1"
check "a patch on an older line moves only that line" "$(tags older-line v0.1.1 v0.1.0 v0.2.0 v0.1.1)" "0.1.1 0.1"
# Publishing an existing tag after a higher patch of its line was tagged, or a
# re-run of its release.
check "an older patch of the line never moves it back" "$(tags backwards v0.1.1 v0.1.1 v0.1.2)" "0.1.1"
check "versions sort as numbers, not text" "$(tags numeric v0.1.10 v0.1.9 v0.1.10)" "0.1.10 0.1"
check "another line's patches do not count" "$(tags lines v0.1.2 v0.1.2 v0.10.5 v1.1.9)" "0.1.2 0.1"
check "a pre-release gets only its own tag" "$(tags pre v0.1.2-rc.1 v0.1.1 v0.1.2-rc.1)" "0.1.2-rc.1"
check "a pre-release does not hold back the line" "$(tags pre-behind v0.1.1 v0.1.2-rc.1 v0.1.1)" "0.1.1 0.1"

dir="$(fixture refused v0.1)"
(cd "$dir" && "$tools/release-tags.sh" v0.1 >/dev/null 2>&1) && r=accepted || r=refused
check "release-tags.sh refuses a version that is not MAJOR.MINOR.PATCH" "$r" refused
(cd "$dir" && "$tools/release-notes.sh" v0.1 >/dev/null 2>&1) && r=accepted || r=refused
check "release-notes.sh refuses a version that is not MAJOR.MINOR.PATCH" "$r" refused

dir="$(fixture notes-first v0.1.0)"
check "the first release lists every commit" "$(notes "$dir" v0.1.0 | sed 's/ ([0-9a-f]*)$//')" \
  "$(printf 'Changes:\n\n- change for v0.1.0\n- first')"

dir="$(fixture notes-since v0.1.0 v0.1.1 v0.1.2-rc.1 v0.1.2)"
check "the notes list the commits since the previous release, pre-releases not counted" \
  "$(notes "$dir" v0.1.2 | sed 's/ ([0-9a-f]*)$//')" \
  "$(printf 'Changes since v0.1.1:\n\n- change for v0.1.2\n- change for v0.1.2-rc.1')"

dir="$(fixture notes-older-line v0.1.0 v0.2.0 v0.10.0)"
check "the previous release is the highest below, by version" \
  "$(notes "$dir" v0.10.0 | head -1)" "Changes since v0.2.0:"

exit "$failed"
