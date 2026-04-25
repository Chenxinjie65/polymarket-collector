param(
    [string]$DataRoot = "data_all_books_jsonl_live",
    [string]$CollectorScript = "scripts/stream_all_books_files.py",
    [double]$CheckIntervalSeconds = 30,
    [int]$DurationSeconds = 0,
    [double]$RestartDelaySeconds = 5,
    [double]$MaxRestartDelaySeconds = 60,
    [double]$StableResetSeconds = 300,
    [double]$IdleReconnectSeconds = 90,
    [double]$MonitorIntervalSeconds = 10,
    [double]$MonitorDurationSeconds = 0,
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

$dataRootAbs = if ([System.IO.Path]::IsPathRooted($DataRoot)) { $DataRoot } else { Join-Path $repoRoot $DataRoot }
$stateDir = Join-Path $dataRootAbs "state"
New-Item -ItemType Directory -Force -Path $stateDir | Out-Null

$stdout = Join-Path $stateDir "guard_stdout.log"
$stderr = Join-Path $stateDir "guard_stderr.log"
$guardPidPath = Join-Path $stateDir "guard.pid"

$existingGuardPid = if (Test-Path $guardPidPath) { Get-Content $guardPidPath -ErrorAction SilentlyContinue | Select-Object -First 1 } else { $null }
if ($existingGuardPid) {
    $existingProc = Get-Process -Id ([int]$existingGuardPid) -ErrorAction SilentlyContinue
    if ($existingProc) {
        throw "Guard is already running with PID $existingGuardPid"
    }
}

$proc = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList @(
        "scripts\books_guard.py",
        "--data-root", $DataRoot,
        "--collector-script", $CollectorScript,
        "--check-interval-seconds", "$CheckIntervalSeconds",
        "--duration-seconds", "$DurationSeconds",
        "--restart-delay-seconds", "$RestartDelaySeconds",
        "--max-restart-delay-seconds", "$MaxRestartDelaySeconds",
        "--stable-reset-seconds", "$StableResetSeconds",
        "--idle-reconnect-seconds", "$IdleReconnectSeconds",
        "--monitor-interval-seconds", "$MonitorIntervalSeconds",
        "--monitor-duration-seconds", "$MonitorDurationSeconds"
    ) `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -WindowStyle Hidden `
    -PassThru

Set-Content -Path $guardPidPath -Value $proc.Id
[pscustomobject]@{
    pid = $proc.Id
    python = $PythonExe
    data_root = $dataRootAbs
    stdout = $stdout
    stderr = $stderr
} | ConvertTo-Json -Compress
