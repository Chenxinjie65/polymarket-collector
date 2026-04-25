param(
    [string]$DataRoot = "data_all_books_jsonl_live",
    [string]$CollectorScript = "scripts/stream_all_books_files.py",
    [int]$DurationSeconds = 0,
    [double]$RestartDelaySeconds = 5,
    [double]$MaxRestartDelaySeconds = 60,
    [double]$StableResetSeconds = 300,
    [double]$IdleReconnectSeconds = 90,
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

$stdout = Join-Path $stateDir "supervisor_stdout.log"
$stderr = Join-Path $stateDir "supervisor_stderr.log"
$pidPath = Join-Path $stateDir "stream.pid"
$supervisorPidPath = Join-Path $stateDir "supervisor.pid"

$existingSupervisorPid = if (Test-Path $supervisorPidPath) { Get-Content $supervisorPidPath -ErrorAction SilentlyContinue | Select-Object -First 1 } else { $null }
if ($existingSupervisorPid) {
    $existingProc = Get-Process -Id ([int]$existingSupervisorPid) -ErrorAction SilentlyContinue
    if ($existingProc) {
        throw "Collector supervisor is already running with PID $existingSupervisorPid"
    }
}

$proc = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList @(
        "scripts\supervise_books_collector.py",
        "--data-root", $DataRoot,
        "--collector-script", $CollectorScript,
        "--duration-seconds", "$DurationSeconds",
        "--restart-delay-seconds", "$RestartDelaySeconds",
        "--max-restart-delay-seconds", "$MaxRestartDelaySeconds",
        "--stable-reset-seconds", "$StableResetSeconds",
        "--idle-reconnect-seconds", "$IdleReconnectSeconds"
    ) `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -WindowStyle Hidden `
    -PassThru

Set-Content -Path $supervisorPidPath -Value $proc.Id
[pscustomobject]@{
    pid = $proc.Id
    python = $PythonExe
    data_root = $dataRootAbs
    supervisor_pid = $proc.Id
    stream_pid_path = $pidPath
    stdout = $stdout
    stderr = $stderr
} | ConvertTo-Json -Compress
