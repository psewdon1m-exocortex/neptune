# Service-owned backup policy

The application Settings page is the only editor of its archive and, where enrolled, mirror schedule. Saturn keeps the authoritative revision. Its central inventory remains available for identities, quotas, enrollment and observation; it no longer edits application schedules or issues their manual backup commands.

The authenticated application backend calls the local Unix socket using its own `X-Neptune-Token`:

- `GET /v1/projects/{deployment}/policy`
- `PUT /v1/projects/{deployment}/policy`
- `GET|POST /v1/projects/{deployment}/policy/runs`

Writes carry a stable `requestId` and `expectedRevision`. Neptune resolves Saturn through Kernel and relays the command with the enrolled producer credential. The returned schema is `exocortex.backup.policy.v1`. A successful authoritative write and a locally applied revision are separate observations. Retrying an uncertain response preserves the operation ID; a conflict requires review of the newer policy. Changing an interval does not issue a manual run.

Archive intervals are hours; existing mirror minute intervals are retained exactly. The Settings field reports an inherited fractional-hour value and allows a new whole-hour value only after explicit editing. Unchanged five-minute mirrors are never rounded to an hour or replaced with a default.

Legacy unversioned schedule/run writers return 426. Upgrade Saturn before Neptune so the new agent can relay policy requests to an implemented upstream endpoint. Upgrade applications with protocol-aware policy UI before exposing the editor. Existing remote desired-state synchronization continues to carry authoritative revisions. The registry retains the existing schedule/next-run metadata and applies updates under its file lock with a durable atomic write. Active accepted transfers keep their execution identity.

Restored policy intent is paused. Resume verifies the enrolled archive source and, for a mirror, its source and destination before applying the new authoritative revision. An authenticated source HEAD must return `X-Neptune-Ready: 1`; HEAD verifies readiness without creating an archive. A failed or uncertain resume leaves the local restore journal intact. Newer restore intent cannot be cleared by an older in-flight request.

The complete grouped-service enrollment profile also requires distinct archive, mirror and resource-reader identities. Reader requests stay scoped to the enrolled root, resolve the current Saturn origin through Kernel, and enforce purpose, range and response-size bounds. Export generations are acknowledged only after remote commitment.

No version number alone proves compatibility. Before deployment, qualify the signed Neptune/Updater/Saturn tuple, including migration of the installed registration and SQLite state. Source tests do not replace Linux/systemd installation and rollback qualification.
