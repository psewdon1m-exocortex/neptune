# Windows packaging

The release workflow publishes a self-contained, signed Windows x64 MSIX. It
requires `WINDOWS_SIGNING_CERTIFICATE_BASE64`, `WINDOWS_SIGNING_CERTIFICATE_PASSWORD`
and `WINDOWS_SIGNING_PUBLISHER` repository secrets. The publisher string must
match the subject of the certificate. The executable, Start entry, taskbar,
installer and window use the Neptune planet icon family.

Run independent local clients with separate protected state and identities:

```powershell
Neptune.Windows.exe --profile work
Neptune.Windows.exe --profile personal
```

Only one process may use a given profile at a time.
