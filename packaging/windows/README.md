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

The **Start with Windows** toggle is stored separately for every profile in the
current user's Windows Run key. It records the current portable executable path,
so enable the toggle again after moving the extracted application directory.
Autostart launches the selected profile minimized and requires no administrator
rights.

Neptune keeps a notification-area icon for its whole lifetime. Closing or
minimizing the window hides it to the tray; use the tray **Exit** command to stop
the client. Portable updates are discovered from Kernel Register and GitHub
Releases, verified against `neptune-windows-release.json` and its SHA-256, then
applied in place by the downloaded Neptune executable after the running process
has exited.
