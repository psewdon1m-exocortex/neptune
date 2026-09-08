# Windows packaging

The current release workflow publishes a self-contained Windows x64 portable
ZIP plus a SHA-256 file and machine-readable manifest. It requires no signing
certificate: extract the archive and run `Neptune.Windows.exe`. The checked-in
MSIX manifest and assets are retained for a future publisher-signed installer.
The executable, taskbar and window use the Neptune planet icon family.

Run independent local clients with separate protected state and identities:

```powershell
Neptune.Windows.exe --profile work
Neptune.Windows.exe --profile personal
```

Only one process may use a given profile at a time.
