# Local development builds only — not used in CI/CD.

TACPLUS := env_var_or_default("TACPLUS_BIN", "tacplus")

default: generate

# Regenerate model/target/generated-sources (not committed) with the compiler
# line tacplus.yaml pins. Generated code is never committed or edited by hand,
# so generation must leave every tracked file as it found it.
generate:
  #!/usr/bin/env bash
  set -euo pipefail
  before="$(git diff HEAD | sha256sum)"
  {{TACPLUS}} build --tag latest
  if [[ "$(git diff HEAD | sha256sum)" != "$before" ]]; then
    echo "generate: tacplus build changed tracked files:" >&2; git status --short >&2; exit 1
  fi

# Nothing under the generated output may be tracked.
check-generated: generate
  #!/usr/bin/env bash
  set -euo pipefail
  if [[ -n "$(git ls-files model/target)" ]]; then
    echo "check-generated: model/target is generated and must not be committed" >&2; exit 1
  fi

# model's antrun step skips tacplus when the generated sources exist, so this
# compiles exactly what generate just wrote.
build: generate
  mvn -B -q install -DskipTests

test: build
  mvn -B test

# The gates every change passes before it is pushed.
gates: check-generated test

clean:
  rm -rf model/target codegen/target server/target target
