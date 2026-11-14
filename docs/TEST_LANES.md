# Test lanes

## rc01 owned Windows journal acceptance

`T10_ActualOwnedJournalResetFencesBothRetainedSubscriptionsWithoutChangingFiles`
is an opt-in `Lane=Live` test. It requires `NEPTUNE_ACCEPTANCE_VOLUME_PROOF`
from the explicitly admitted, dedicated rc01 lane-B NTFS image mounted as `S:`.
The test validates its volume GUID and ownership marker before invoking the
native journal reset. Never point this fixture at a system or working volume.
It creates 10,000 files and 1,024 directories, reopens qualified subscriptions
three times, verifies independent cursor fencing after the actual journal-ID
change, and reads back every source SHA. This proves native gap detection and
source preservation; it does not qualify remote publication recovery or the
historical C01 workload.

The rc01 delete tests in `ReleaseAcceptanceDeleteApprovalTests.cs` exercise
approval-response loss, a later gap, and a 32-file stale/current approval plan
using native NTFS and the test remote. They do not replace the production API,
trusted TLS, signed updater or Linux/systemd acceptance lanes.

Reviewed quarantine-delete recovery keeps the original operation when only a
duplicate watcher hint advances: native journal generation, incarnation,
baseline/CAS, exclusions, no-active-work and qualified complete absence remain
required. Its receipt acknowledges only the original dirty pair. Later hints
on an already published deletion need a new full native membership proof and
a fresh remote feed showing absence. That proof retires at most 120 unchanged
dirty pairs per pass; a watcher generation arriving after the proof's input
snapshot stays pending for another proof. The original request and entry JSON
are retained byte-for-byte. The targeted T04 cases include that race and
normal pending-debt settlement. The native-source move and excluded-policy
negative cases exercise refusal before approval or policy admission respectively;
they do not claim those transitions executed after approval.

The121-entry boundary case verifies two new full native proofs, zero pending
entries and no extra proof on the next settled NOOP. Each eligible batch may
enumerate the complete native root; this cost requires B01 measurement before
claiming preserved speed. Historical membership must not recreate conflicts
for settled terminal deletions with an unchanged acknowledged dirty pair.

`ReleaseAcceptanceSnapshotBoundaryTests` is a separate opt-in live snapshot
fixture. `NEPTUNE_ACCEPTANCE_SNAPSHOT_ROWS=1000` selects its small setup probe;
`1004282` streams the complete catalog through the production wire parser and
durable store with checks at999999/1000000/1000001/1004282. It reopens the actual
native profile after a partial page and at one million rows, verifies every
stored descriptor and a manifest SHA, and retains its owned database on PASS
or FAIL. Pages contain at most500 entries and storage subpages at most120.
This fixture creates no physical million-file source and makes no API/TLS or
full filesystem synchronization claim. Separate parser negatives reject
truncated bodies, duplicate nodes and changed/empty continuation cursors.

`ReleaseAcceptanceDiskFullTests` requires an explicitly admitted NEW owned
NTFS volume proof in `NEPTUNE_PRESSURE_VOLUME_PROOF`. Its 512 MiB volume is
separate from working, retained and journal-test volumes. The test verifies
the volume GUID/ownership marker before filling it or deleting its sole filler.
It retains an accepted backup run and sealed payload, reaches real OS disk-full,
requires a failed production store update with no changed committed DB/payload,
then removes only its filler and reopens the same profile. The production
backup uploader resumes the same original/idempotency key from its accepted
offset against an in-memory test transport and verifies full byte/SHA readback.
This is store-level pressure/recovery evidence, not actual API E2E. Keep the
fixture profile/payload and all failed attempts; do not target any system or
retained volume. The rc01 runner enforces a 512 MiB process/job aggregate limit.

Run from the Neptune repository with the SDK pinned by `global.json`.

## Portable and native baseline

```sh
dotnet test tests/Neptune.Core.Tests/Neptune.Core.Tests.csproj -c Release --filter "Lane!=Live" --logger trx
```

CI runs this baseline on both Windows and Linux. Tests requiring NTFS, ACL, DPAPI or Windows identity report SKIP on Linux; SKIP is not native qualification. Their bodies still execute on Windows. A full baseline was not rerun during the 2026-10-16 remediation: only previously failing methods and affected regressions were repeated.

The native child helper is built through the test project's project reference. It uses the same Debug/Release configuration as the parent test assembly and `DOTNET_HOST_PATH` (or `dotnet` on PATH). A standalone checkout is supported. The shared codec vectors are shipped in `TestData/name-codec-v1.json`; when Saturn's source fixture is available it is preferred. Keep the shipped bytes identical to the reviewed Saturn contract.

Wire evidence defaults to `TestResults/stage3-wire` under the test output directory. Set `NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT` to preserve it in an audit directory. Native test source/state roots are disposable; never point them at a working vault.

## Live and package qualification

`Lane=Live` identifies tests requiring an explicitly prepared API, retained profile or published Windows payload. Do not run this lane against production or a working profile. Select the relevant class after preparing its fixture, rather than running every live test with one shared environment:

```sh
dotnet test tests/Neptune.Core.Tests/Neptune.Core.Tests.csproj -c Release --filter "FullyQualifiedName~CompletionServiceTests" --logger trx
```

Completion API tests require `COMPLETION_EVIDENCE` containing the owned API manifest expected by that class. Retained-parent tests additionally require `ENDURANCE_RETAINED_PARENT_DB` and `ENDURANCE_RETAINED_PARENT_EVIDENCE`. The in-place Windows payload test requires `ENDURANCE_WINDOWS_PAYLOAD`; it is separate from an OS-trusted MSI install/update/rollback. Missing fixture inputs must fail the selected live test, not silently pass.

The baseline CI filter does not qualify these live scenarios for release. Preserve live receipts, fixture custody, source/binary digests and OS trust results in the release evidence gate.

## Historical evidence

- `B01_HistoricalCopyRetainsEvidenceAndReportsQualifiedBlocker`: set `NEPTUNE_HISTORICAL_DATABASE` to an offline retained profile with its WAL/SHM companions. Only a disposable copy is opened. The test checks preserved evidence and bounded frontier behavior; it does not establish complete replay of the historical workload.
- `B12_PinnedHistoricalWriterRefusesAcceptedOriginalsWithoutChangingDatabaseOrWal`: set `NEPTUNE_HISTORICAL_CORE` to the predecessor DLL with SHA-256 `b3c26bf78f3c7a6329f1f97144de804f345fb192f749da9355988f729e273fc5`. It checks refusal and unchanged DB/WAL, rather than accepting an arbitrary older build.

Without these inputs the two tests report SKIP / NOT_RUN. Explicit invalid inputs fail. The original artifacts were removed by earlier cleanup and were unavailable on 2026-10-16. Current backup tests still validate WAL-aware copies and original recovery independently; this does not replace historical compatibility evidence.

## Latest local evidence

Parent workspace: `.audit/2026-10-16/pre-release-remediation/REPORT.md`; original snapshot: `.audit/2026-10-16/pre-release-snapshot/REPORT.md`. The report records individual attempts, fixes and final targeted outcomes. Production qualification remains separate and must bind the exact immutable release candidate.
