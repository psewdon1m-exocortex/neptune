[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ToolDirectory
)
$ErrorActionPreference = 'Stop'
$lockPath = Join-Path $PSScriptRoot '../packaging/windows/packaging-tools.lock.json'
$packagingLock = Get-Content -LiteralPath $lockPath -Raw | ConvertFrom-Json
if ($packagingLock.schema -ne 'exocortex.neptune.windows-packaging-tools.v1' -or
    $packagingLock.version -ne '4.0.6' -or $packagingLock.license -ne 'MS-RL') {
    throw 'Unknown Windows packaging tool lock.'
}
$toolRoot = [IO.Path]::GetFullPath($ToolDirectory)
New-Item -ItemType Directory -Path $toolRoot -Force | Out-Null
$archivePath = Join-Path $toolRoot 'wix.nupkg'
if (!(Test-Path -LiteralPath $archivePath)) {
    Invoke-WebRequest -Uri $packagingLock.package_url -OutFile $archivePath
}
if ((Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $packagingLock.package_sha256) {
    throw 'WiX package checksum differs from the pinned package. No tools were executed.'
}
Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [IO.Compression.ZipFile]::OpenRead($archivePath)
try {
    foreach ($entry in $archive.Entries) {
        $target = [IO.Path]::GetFullPath((Join-Path $toolRoot $entry.FullName))
        if (!$target.StartsWith($toolRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
            throw 'WiX package contains a path outside the tool directory.'
        }
        if (!$entry.Name) { continue }
        New-Item -ItemType Directory -Path ([IO.Path]::GetDirectoryName($target)) -Force | Out-Null
        # Re-extract the verified archive: an existing modified cache must not be executed.
        [IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $target, $true)
    }
} finally { $archive.Dispose() }
$entrypoint = Join-Path $toolRoot $packagingLock.entrypoint
if (!(Test-Path -LiteralPath $entrypoint)) { throw 'Pinned WiX entrypoint is absent.' }
Write-Output $entrypoint
