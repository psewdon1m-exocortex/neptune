# neptune deployment and recovery contract

Linux archive export, independent Volt single-file mirror and Windows folder synchronization are separate pipelines. Linux consumes schedules and remote commands from Saturn. A command succeeds only after the matching pipeline and receipt complete. Run IDs are deterministic for remote archive commands; interrupted upload resumes the same spool and nonzero Saturn offset. Concurrent recovery and command replay wait for the same result. Windows stores credentials with per-user DPAPI, isolates profiles, preserves Unicode names and mirrors renames/deletions; an incomplete or inaccessible local tree aborts before remote cleanup. Only Register references are cached. A new Windows identity must re-enroll protected credentials.

## Trust and operator prerequisites

The selected deployment profile contains Kernel, Volt, Saturn, Updater, Neptune and Gryphon. Per-host helpers are reused when healthy; attaching a service does not silently downgrade or reinstall them. Operator control is available through connected service Settings and typed CLI actions. Jobs retain their identifiers across page reloads and must reach a verified terminal result.

Release manifests use detached RSA-PSS-SHA256 signatures with a per-project RSA key of at least 3072 bits. Use scripts/create-release-key.mjs to generate operator-owned signing material and keep the private key only in protected CI Secrets. A clean-host HTTPS bootstrap obtains the public counterpart from the selected GitHub release, verifies the manifest and pins the key locally; an existing key is never replaced automatically. Saturn also retains its Ed25519 installer signature. The six-service head bundles require Updater 0.4.2 or newer.

Populate actual Kernel/Volt bootstrap coordinates, service tokens, SFTP host fingerprint and deployment CIDRs. First-install HTTPS bootstraps provision and pin verified public release keys automatically; existing pinned keys are never replaced automatically. Secrets must not appear in links, responses, browser persistence or logs. Crawler directives supplement authenticated/private access; they do not hide public data from an uncooperative crawler. Resolve service data and generated link origins through Kernel; bootstrap trust and local loopback helper endpoints are explicit exceptions.

## Recovery boundaries

Keep the Access Key and helper-recovery passphrase separately from their archives. Main-service recovery retains user settings and application data while preserving or requiring re-enrollment of external host trust. The encrypted helper profile is controlled by Updater and contains Neptune/Gryphon state and credentials plus Updater job/rollback history. It excludes executable files, release trust keys, systemd units and head deployment environments. Install trusted software and register target heads before restoring. The bounded helper archive fails explicitly at 128 MiB expanded or 10000 files; it never silently omits data.

## Acceptance evidence

The seven-area policy in .github/pre-push-gate.json is required after native CI verification. Public indexing is intentionally not applicable. For an uncommitted local review run the gate with --worktree after the native checks. Gate PASS checks policy/evidence/verification linkage; it is not a substitute for executing the integration scenarios.

Qualify the connected system with real HTTP Kernel→Volt authentication, clean archives/restores, PostgreSQL and pinned SFTP, independent Volt mirror, Windows folder synchronization, network interruption/replay, signed artifact rejection, private-edge negative cases and helper installation/reuse. Record PASS, FAIL and NOT_RUN separately. Production credentials, signed publication and actual deployment remain operator provisioning operations.

See [README](README.md) for service commands.
