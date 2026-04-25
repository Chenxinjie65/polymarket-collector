param(
    [string]$TaskPrefix = "PolymarketBooks"
)

$ErrorActionPreference = "Stop"

$taskNames = @(
    "$TaskPrefix-Guard-Startup",
    "$TaskPrefix-Guard-Logon"
)

foreach ($taskName in $taskNames) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
}

[pscustomobject]@{
    removed = $taskNames
} | ConvertTo-Json -Compress
