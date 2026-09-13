# neptune deployment and recovery contract

This service-local record is subordinate to the coordinated
[Part 11 deployment profile](https://github.com/psewdon1m-exocortex/general/blob/main/PART_11_INITIAL_MULTI_SERVICE_DEPLOYMENT.md)
and the shared-agent contracts in
[Part 09](https://github.com/psewdon1m-exocortex/general/blob/main/PART_09_SERVICE_AGENTS_DEPLOYMENT_AND_LIFECYCLE.md) and
[Part 10](https://github.com/psewdon1m-exocortex/general/blob/main/PART_10_SERVICE_AGENTS_UI_AND_OPERATOR_WORKFLOWS.md).

Linux archive export, independent Volt single-file mirror and Windows folder synchronization are separate pipelines. Linux consumes schedules and remote commands from Saturn. A command succeeds only after the matching pipeline and receipt complete. Run IDs are deterministic for remote archive commands; interrupted upload resumes the same spool and nonzero Saturn offset. Concurrent recovery and command replay wait for the same result. Windows stores credentials with per-user DPAPI, isolates profiles, preserves Unicode names and mirrors renames/deletions; an incomplete or inaccessible local tree aborts before remote cleanup. Only Register references are cached. A new Windows identity must re-enroll protected credentials.

## Trust and operator prerequisites

The selected deployment profile contains Kernel, Volt, Saturn, Updater, Neptune and Gryphon. Per-host helpers are reused when healthy; attaching a service does not silently downgrade or reinstall them. Operator control is available through connected service Settings and typed CLI actions. Jobs retain their identifiers across page reloads and must reach a verified terminal result.

Release manifests use detached RSA-PSS-SHA256 signatures with per-product private keys kept only in GitHub Secrets and exposed only to protected release-signing jobs. CI derives the public counterparts. Neptune Linux trust is pinned from inside the installer verified by Updater's exact-version bootstrap; Windows carries its own native trust. Updater creates `/etc/exocortex/release-trust/neptune.pem` before any Linux helper download and never replaces an existing mismatching key automatically. No `scp`, manual release-key fingerprint or public key downloaded beside a helper manifest is part of this trust path. Saturn also retains its Ed25519 installer signature. The six-service head bundles require Updater 0.4.3 or newer.

Populate actual Kernel/Volt coordinates and service tokens. Neptune Linux keeps its own mode-`0600` `/etc/neptune/neptune.env`; no head service may absorb it. Neptune exposes no browser login and requires no inbound public port, so `OPERATOR_CIDR` is not an authentication mechanism here. Secrets must not appear in links, responses, browser persistence or logs. Crawler directives supplement authenticated access; they do not hide public data from an uncooperative crawler. Resolve service data and generated link origins through Kernel; bootstrap trust and local loopback helper endpoints are explicit exceptions.

## Recovery boundaries

Keep the Access Key and helper-recovery passphrase separately from their archives. Main-service recovery retains user settings and application data while preserving or requiring re-enrollment of external host trust. The encrypted helper profile is controlled by Updater and contains Neptune/Gryphon state and credentials plus Updater job/rollback history. It excludes executable files, release trust keys, systemd units and head deployment environments. Install trusted software and register target heads before restoring. The bounded helper archive fails explicitly at 128 MiB expanded or 10000 files; it never silently omits data.

## Acceptance evidence

The seven-area policy in .github/pre-push-gate.json is required after native CI verification. Public indexing is intentionally not applicable. For an uncommitted local review run the gate with --worktree after the native checks. Gate PASS checks policy/evidence/verification linkage; it is not a substitute for executing the integration scenarios.

Qualify the connected system with real HTTP Kernel→Volt authentication, clean archives/restores, PostgreSQL and pinned SFTP, independent Volt mirror, Windows folder synchronization, network interruption/replay, signed artifact rejection, private-edge negative cases and helper installation/reuse. Record PASS, FAIL and NOT_RUN separately. Production credentials, signed publication and actual deployment remain operator provisioning operations.

See [README](README.md) for service commands.
