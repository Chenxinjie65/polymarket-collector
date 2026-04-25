param(
    [string]$DataRoot = "data_all_books_jsonl_live",
    [string]$CollectorScript = "scripts/stream_all_books_files.py",
    [string]$TaskPrefix = "PolymarketBooks",
    [double]$CheckIntervalSeconds = 30,
    [double]$MonitorIntervalSeconds = 10,
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not $PythonExe) {
    $pythonCmd = Get-Command python -ErrorAction Stop
    $pythonwCandidate = Join-Path (Split-Path -Parent $pythonCmd.Source) "pythonw.exe"
    if (Test-Path $pythonwCandidate) {
        $PythonExe = $pythonwCandidate
    } else {
        $PythonExe = $pythonCmd.Source
    }
}

$scriptPath = Join-Path $repoRoot "scripts\start_books_guard_hidden.ps1"
$commonArgs = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$scriptPath`" -DataRoot `"$DataRoot`" -CollectorScript `"$CollectorScript`" -CheckIntervalSeconds $CheckIntervalSeconds -MonitorIntervalSeconds $MonitorIntervalSeconds -PythonExe `"$PythonExe`""
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances IgnoreNew

$startupTaskName = "$TaskPrefix-Guard-Startup"
$startupAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $commonArgs
$startupTrigger = New-ScheduledTaskTrigger -AtStartup
$startupPrincipal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
Register-ScheduledTask -TaskName $startupTaskName -Action $startupAction -Trigger $startupTrigger -Principal $startupPrincipal -Settings $settings -Force | Out-Null

$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$logonTaskName = "$TaskPrefix-Guard-Logon"
$logonAction = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $commonArgs
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $currentUser
$logonPrincipal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Highest
Register-ScheduledTask -TaskName $logonTaskName -Action $logonAction -Trigger $logonTrigger -Principal $logonPrincipal -Settings $settings -Force | Out-Null

[pscustomobject]@{
    startup_task = $startupTaskName
    logon_task = $logonTaskName
    script = $scriptPath
    data_root = $DataRoot
    python = $PythonExe
} | ConvertTo-Json -Compress
