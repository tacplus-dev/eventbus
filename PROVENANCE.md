# Provenance

Copied **wholesale** on 2026-10-23 from `gitlab.com/seabeam/schematic/platform/services/eventbus` at commit
`ea83ea2` (its own nested git repository; history carried over), with `proto/services/eventbus.proto` and
`proto/dependencies.yaml` from `platform/proto` at `ca7c322` on branch `big8-p1-messaging`. Build outputs not copied.

Jody, 2026-10-23: *"We will copy kernel over to its github home and only make changes to it. we will
keep these as ships in the night for now. For the new stuff we want to do it the right way."* and
*"the following will also need to be ported over to tacplus-dev ... blobstore gateway kmaker letsencrypt
eventbus oauth"*. The private copy runs the launch fleet unchanged; new-design changes land here.

Divergences: headers in tacplus.yaml (Jody Scott, Apache-2.0; LICENSE added); `src: proto` (this repo
holds the service's own proto); `proto/registry.yaml` is a lock of github.com/tacplus-dev/registry.

2026-10-27, no SeaBeam (#4): tacplus-dev works by itself, so nothing outside this note and dated records
names SeaBeam. The eventbus proto is this repository's own (`proto/services/eventbus.proto`), not vendored or
fetched: its contract in the registry lock is `github.com/tacplus-dev/eventbus`, and kernel, letsencrypt and
their depends come from their tacplus-dev homes through the registry. The Java package of the server, the
codegen plugin and the generated model (`pkg`), and the Maven groupId, are `com.github.tacplusdev.eventbus`
(was `com.gitlab.seabeam.controlplane.schematic.services.eventbus`; the codegen was
`com.gitlab.seabeam.controlplane.schematic.codegen`). Removed: `.gitlab-ci.yml` (SeaBeam's GitLab pipeline
and registry; release CI is #2), the GitLab Maven package repository and distributionManagement in the POMs,
and the GitLab project id and Rust interfaces crate in `proto/dependencies.yaml` (tacplus still requires
`registry.imports.java` for a Java build, now `com.github.tacplusdev.kernel`). Source headers and image labels
name Jody Scott and tacplus.dev; comments no longer cite SeaBeam's products.
