[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$PayloadDirectory,
    [Parameter(Mandatory)][string]$OutputDirectory,
    [Parameter(Mandatory)][string]$Version,
    [Parameter(Mandatory)][string]$SourceRevision,
    [string]$Repository = 'psewdon1m-exocortex/neptune',
    [string]$DotnetPath = 'dotnet',
    [string]$PythonPath = 'python',
    [switch]$ForRelease
)
$ErrorActionPreference = 'Stop'
if ($env:OS -ne 'Windows_NT') { throw 'MSI authoring/extraction requires Windows.' }
$output = [IO.Path]::GetFullPath($OutputDirectory)
if (Test-Path -LiteralPath $output) {
    if (Get-ChildItem -LiteralPath $output -Force | Select-Object -First 1) {
        throw 'Package output must be empty; refuse to mix packages from different builds.'
    }
} else { New-Item -ItemType Directory -Path $output | Out-Null }
$packager = Join-Path $PSScriptRoot '../packaging/windows/windows_release.py'
if ($ForRelease) {
    & $PythonPath $packager require-release-readiness --output (Join-Path $PSScriptRoot '../packaging/windows/release-readiness.json')
    if ($LASTEXITCODE -ne 0) { throw 'Windows release acceptance is incomplete.' }
}
$wix = & (Join-Path $PSScriptRoot 'restore-windows-packaging-tools.ps1') -ToolDirectory (Join-Path $output 'tools')
$builderVersion = & $DotnetPath $wix --version
if ($LASTEXITCODE -ne 0 -or $builderVersion -notmatch '^4\.0\.6\+73c89738$') { throw 'Unexpected WiX builder version.' }
& $PythonPath $packager prepare --payload $PayloadDirectory --output $output --version $Version --revision $SourceRevision
if ($LASTEXITCODE -ne 0) { throw 'Shared payload preparation failed.' }
$installer = Join-Path $output "neptune-windows-$Version-win-x64.msi"
& $DotnetPath $wix build (Join-Path $output 'Neptune.wxs') -arch x64 -o $installer -intermediateFolder (Join-Path $output 'intermediate') -wx
if ($LASTEXITCODE -ne 0) { throw 'MSI compilation or validation failed.' }
$decompiled = Join-Path $output 'verification/Neptune.wxs'
New-Item -ItemType Directory -Path (Split-Path $decompiled) -Force | Out-Null
# Extract the cabinet with WiX, without msiexec, registration, installation, or application launch.
& $DotnetPath $wix msi decompile $installer -x (Join-Path $output 'verification/extracted') -o $decompiled
if ($LASTEXITCODE -ne 0) { throw 'MSI payload extraction failed.' }
& (Join-Path $PSScriptRoot 'verify-windows-msi.ps1') -InstallerPath $installer -Version $Version `
    -EvidencePath (Join-Path $output 'verification/msi-inspection.json')
& $PythonPath $packager finalize --output $output --repository $Repository --tag "neptune-v$Version" --decompiled $decompiled
if ($LASTEXITCODE -ne 0) { throw 'MSI/ZIP identity verification failed.' }
& $PythonPath $packager verify --output $output --version $Version --repository $Repository --revision $SourceRevision
if ($LASTEXITCODE -ne 0) { throw 'Windows release manifest verification failed.' }
Write-Output "Windows packages built from one payload: $output"
