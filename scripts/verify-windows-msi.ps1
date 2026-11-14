[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$InstallerPath,
    [Parameter(Mandatory)][string]$Version,
    [Parameter(Mandatory)][string]$EvidencePath
)
$ErrorActionPreference = 'Stop'
# Read-only MSI database inspection. Never run an install/admin-install sequence.
$installer = New-Object -ComObject WindowsInstaller.Installer
$database = $installer.OpenDatabase([IO.Path]::GetFullPath($InstallerPath), 0)
$properties = @{}
$view = $database.OpenView('SELECT * FROM Property')
$view.Execute()
while ($row = $view.Fetch()) { $properties[$row.StringData(1)] = $row.StringData(2) }
$view.Close()
if ($properties.ProductVersion -ne $Version -or
    $properties.UpgradeCode -ne '{D75C847C-C5AA-5E77-BC1F-24495C372245}' -or $properties.ContainsKey('ALLUSERS')) {
    throw 'MSI product version, upgrade identity or per-user scope is invalid.'
}
$summary = $database.SummaryInformation(0)
if (([int]$summary.Property(15) -band 8) -eq 0 -or $summary.Property(7) -notlike 'x64;*') {
    throw 'MSI must be x64 and marked as not requiring elevated privileges.'
}
$sequences = @{}
$view = $database.OpenView('SELECT * FROM InstallExecuteSequence')
$view.Execute()
while ($row = $view.Fetch()) { $sequences[$row.StringData(1)] = [int]$row.StringData(3) }
$view.Close()
if ($sequences.RemoveExistingProducts -le $sequences.InstallInitialize -or
    $sequences.RemoveExistingProducts -ge $sequences.InstallFinalize) {
    throw 'Removal of the previous MSI must be inside the rollback transaction.'
}
$registration = $false
$view = $database.OpenView('SELECT * FROM Registry')
$view.Execute()
while ($row = $view.Fetch()) {
    if ($row.StringData(2) -ne '1' -or $row.StringData(3) -notlike 'Software\Exocortex\Neptune\Installer*') {
        throw 'MSI owns a registry entry outside its HKCU installer registration.'
    }
    if ($row.StringData(4) -eq 'InstallDirectory' -and $row.StringData(5) -eq '[INSTALLDIR]') { $registration = $true }
}
$view.Close()
if (!$registration) { throw 'MSI update guard registration is absent.' }
$view = $database.OpenView('SELECT * FROM _Tables')
$view.Execute()
while ($row = $view.Fetch()) {
    if ($row.StringData(1) -eq 'CustomAction') { throw 'This MSI increment must not embed custom actions.' }
}
$view.Close()
$directories = @{}
$view = $database.OpenView('SELECT * FROM Directory')
$view.Execute()
while ($row = $view.Fetch()) {
    $directories[$row.StringData(1)] = @{ parent = $row.StringData(2); name = $row.StringData(3).Split('|')[-1] }
}
$view.Close()
if ($directories.INSTALLDIR.parent -ne 'ExocortexPrograms' -or $directories.INSTALLDIR.name -ne 'Neptune' -or
    $directories.ExocortexPrograms.parent -ne 'UserPrograms' -or $directories.UserPrograms.parent -ne 'LocalAppDataFolder') {
    throw 'MSI application directory does not use the approved per-user application location.'
}
$evidence = @{
    schema = 'exocortex.neptune.windows-msi-inspection.v1'
    status = 'PASS'
    version = $Version
    product_code = $properties.ProductCode
    upgrade_code = $properties.UpgradeCode
    installer_sha256 = (Get-FileHash -LiteralPath $InstallerPath -Algorithm SHA256).Hash.ToLowerInvariant()
    per_user = $true
    limited_privileges = $true
    rollback_sequence = $sequences.RemoveExistingProducts
    registry_scope = 'HKCU installer registration only; no profile or Run-key writes'
    custom_actions = 0
    inspection_mode = 'read-only database; no installation or application launch'
}
$evidence | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $EvidencePath -Encoding utf8
Write-Output 'PASS read-only MSI scope, upgrade transaction, profile isolation and update guard registration'
