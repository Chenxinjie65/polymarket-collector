param(
    [string]$DataRoot = "data_all_books_jsonl_live"
)

$ErrorActionPreference = "SilentlyContinue"

$repoRoot = Split-Path -Parent $PSScriptRoot
$dataRootAbs = if ([System.IO.Path]::IsPathRooted($DataRoot)) { $DataRoot } else { Join-Path $repoRoot $DataRoot }
$stateDir = Join-Path $dataRootAbs "state"

$pidFiles = @(
    "guard.pid",
    "monitor.pid",
    "supervisor.pid",
    "stream.pid"
)

foreach ($pidFile in $pidFiles) {
    $path = Join-Path $stateDir $pidFile
    if (-not (Test-Path $path)) { continue }
    $pidValue = Get-Content $path | Select-Object -First 1
    if (-not $pidValue) { continue }
    Stop-Process -Id ([int]$pidValue) -Force
}

[pscustomobject]@{
    data_root = $dataRootAbs
    stopped = $pidFiles
} | ConvertTo-Json -Compress
