param([ValidateSet('Start','Stop')][string]$Mode)
$ErrorActionPreference='Stop'
if($env:GITHUB_ACTIONS -ne 'true' -or $env:RUNNER_OS -ne 'Windows' -or $env:GITHUB_REPOSITORY -ne 'psewdon1m-exocortex/neptune' -or $env:GITHUB_REF -notlike 'refs/heads/release-qualification/package3-*' -or $env:GITHUB_SHA -notmatch '^[a-f0-9]{40}$'){throw 'Isolated qualification CI only'}
$root=Join-Path $env:RUNNER_TEMP 'package3-native-volume';$statePath=Join-Path $root 'ownership.json'
function Save($s){[IO.File]::WriteAllText($statePath,($s|ConvertTo-Json -Depth 8),[Text.UTF8Encoding]::new($false))}
if($Mode -eq 'Start'){
 if((Test-Path -LiteralPath $root) -or (Test-Path -LiteralPath 'R:\') -or (Get-Volume -DriveLetter R -ErrorAction SilentlyContinue)){throw 'Fresh owned directory and unused R: required'}
 $null=New-Item -ItemType Directory -Path $root
 $file=Join-Path $root 'package3-native-ntfs.vhdx';$s=@{at=(Get-Date -AsUTC -Format o);status='PREPARING';image=$file;drive='R';commit=$env:GITHUB_SHA;runId=$env:GITHUB_RUN_ID;job=$env:GITHUB_JOB;maximumMiB=8192;productOrTestChanges=0}
 Save $s
 try{
  $commands=@(
   ('create vdisk file="{0}" maximum=8192 type=expandable' -f $file)
   ('select vdisk file="{0}"' -f $file)
   'attach vdisk'
   'create partition primary'
   'format fs=ntfs label=ExocortexP3 quick'
   'assign letter=R'
  )
  $commandPath=Join-Path $root 'create.diskpart';[IO.File]::WriteAllLines($commandPath,$commands,[Text.Encoding]::ASCII)
  $output=& diskpart.exe /s $commandPath;$code=$LASTEXITCODE;$output|Set-Content -LiteralPath (Join-Path $root 'create.log');if($code -ne 0){throw 'DiskPart create failed'}
  $image=Get-DiskImage -ImagePath $file;$disk=$image|Get-Disk;$part=Get-Partition -DriveLetter R;$volume=Get-Volume -DriveLetter R
  if(-not $image.Attached -or $part.DiskNumber -ne $disk.Number -or $volume.FileSystem -ne 'NTFS' -or $volume.FileSystemLabel -ne 'ExocortexP3'){throw 'Exact attached private NTFS identity required'}
  $s.diskNumber=$disk.Number;$s.diskUniqueId=$disk.UniqueId;$s.volumeUniqueId=$volume.UniqueId;Save $s
  $journal=& fsutil.exe usn createjournal m=33554432 a=8388608 R:;$code=$LASTEXITCODE;$journal|Set-Content -LiteralPath (Join-Path $root 'journal-create.log');if($code -ne 0){throw 'Private USN journal creation failed'}
  $query=& fsutil.exe usn queryjournal R:;$code=$LASTEXITCODE;$query|Set-Content -LiteralPath (Join-Path $root 'journal-query.log');if($code -ne 0){throw 'Private USN journal unavailable'}
  $fixtures='R:\qualification-fixtures';$null=New-Item -ItemType Directory -Path $fixtures
  # Establish ownership on this newly created fixture, before any test runs.
  # The reduced-token tests retain their actual user SID; no shared ACL changes.
  $owner=[Security.Principal.WindowsIdentity]::GetCurrent().User;$acl=[Security.AccessControl.DirectorySecurity]::new();$acl.SetOwner($owner);$acl.SetAccessRuleProtection($true,$false)
  foreach($who in @($owner.Value,'S-1-5-18')){$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new([Security.Principal.SecurityIdentifier]::new($who),[Security.AccessControl.FileSystemRights]::FullControl,[Security.AccessControl.InheritanceFlags]'ContainerInherit,ObjectInherit',[Security.AccessControl.PropagationFlags]::None,[Security.AccessControl.AccessControlType]::Allow))}
  Set-Acl -LiteralPath $fixtures -AclObject $acl
  $evidence=Join-Path $root 'native-evidence';$null=New-Item -ItemType Directory -Path $evidence
  "NEPTUNE_FRESH_CYCLE_TEST_ROOT=$fixtures"|Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append
  "NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT=$evidence"|Out-File -FilePath $env:GITHUB_ENV -Encoding utf8 -Append
  $s.status='READY_PRIVATE_NTFS_WITH_OWN_USN_JOURNAL';$s.fixtureRoot=$fixtures;$s.fixtureOwnerSID=$owner.Value;$s.isolation='Separate native volume; all journal/hardlink/UNKNOWN guards and assertions unchanged';Save $s
  Write-Output ($s|ConvertTo-Json -Depth 8 -Compress)
 }catch{$s.status='FAIL_SETUP';$s.error=$_.Exception.Message;Save $s;throw}
}else{
 if(-not (Test-Path -LiteralPath $statePath)){throw 'No owned volume record; no detach attempted'}
 $s=Get-Content -LiteralPath $statePath -Raw|ConvertFrom-Json -AsHashtable
 if($s.image -ne (Join-Path $root 'package3-native-ntfs.vhdx') -or $s.commit -ne $env:GITHUB_SHA -or $s.runId -ne $env:GITHUB_RUN_ID -or $s.drive -ne 'R'){throw 'Ownership record differs; no detach attempted'}
 $image=Get-DiskImage -ImagePath $s.image
 if($image.Attached){$disk=$image|Get-Disk;if($null -ne $s.diskUniqueId -and $disk.UniqueId -ne $s.diskUniqueId){throw 'Disk incarnation differs'};Dismount-DiskImage -ImagePath $s.image -ErrorAction Stop|Out-Null}
 if((Get-DiskImage -ImagePath $s.image).Attached){throw 'Owned image still attached'}
 $s.detached=$true;$s.finishedAt=Get-Date -AsUTC -Format o;Save $s
 Write-Output 'Owned qualification volume detached; logs and input image retained for CI review.'
}
