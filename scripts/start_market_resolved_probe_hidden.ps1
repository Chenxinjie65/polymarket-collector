param(
    [string]$DataRoot = "data_all_books_jsonl_live",
    [int]$AssetCount = 3,
    [string]$PythonExe = "C:\Users\ztmy\AppData\Local\Programs\Python\Python313\pythonw.exe"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$dataRootAbs = if ([System.IO.Path]::IsPathRooted($DataRoot)) { $DataRoot } else { Join-Path $repoRoot $DataRoot }
$stateDir = Join-Path $dataRootAbs "state\market_resolved_probe"
New-Item -ItemType Directory -Force -Path $stateDir | Out-Null

$stdout = Join-Path $stateDir "probe_stdout.log"
$stderr = Join-Path $stateDir "probe_stderr.log"
$pidPath = Join-Path $stateDir "probe.pid"

$existingPid = if (Test-Path $pidPath) { Get-Content $pidPath -ErrorAction SilentlyContinue | Select-Object -First 1 } else { $null }
if ($existingPid) {
    $existingProc = Get-Process -Id ([int]$existingPid) -ErrorAction SilentlyContinue
    if ($existingProc) {
        throw "market_resolved probe is already running with PID $existingPid"
    }
}

$proc = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList @(
        "scripts\probe_market_resolved.py",
        "--data-root", $DataRoot,
        "--asset-count", "$AssetCount"
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
