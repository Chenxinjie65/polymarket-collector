param(
    [string]$DataRoot = "data_all_books_jsonl_live",
    [double]$IntervalSeconds = 10,
    [double]$DurationSeconds = 0,
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

$stdout = Join-Path $stateDir "monitor_stdout.log"
$stderr = Join-Path $stateDir "monitor_stderr.log"
$pidPath = Join-Path $stateDir "monitor.pid"

$existingPid = if (Test-Path $pidPath) { Get-Content $pidPath -ErrorAction SilentlyContinue | Select-Object -First 1 } else { $null }
if ($existingPid) {
    $existingProc = Get-Process -Id ([int]$existingPid) -ErrorAction SilentlyContinue
    if ($existingProc) {
        throw "Monitor is already running with PID $existingPid"
    }
}

$proc = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList @(
        "scripts\monitor_books_runtime.py",
        "--data-root", $DataRoot,
        "--interval-seconds", "$IntervalSeconds",
        "--duration-seconds", "$DurationSeconds"
    ) `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -WindowStyle Hidden `
    -PassThru

Set-Content -Path $pidPath -Value $proc.Id
[pscustomobject]@{
    pid = $proc.Id
    python = $PythonExe
    data_root = $dataRootAbs
    stdout = $stdout
    stderr = $stderr
} | ConvertTo-Json -Compress
