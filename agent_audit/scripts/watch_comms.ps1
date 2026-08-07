param(
    [string[]]$Paths = @(
        "AGENT_COMMS.md",
        "agent_audit\LOCKS.md",
        "agent_audit\TASKS.md",
        "agent_audit\STATUS.md",
        "agent_audit\comms\CHANNEL.md",
        "agent_audit\agents\Codex.md",
        "agent_audit\agents\claude_code.md"
    ),
    [string]$LogPath = "agent_audit\comms_alerts.log",
    [int]$PollSeconds = 2
)

$ErrorActionPreference = "Stop"
$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $repo

$logFullPath = Join-Path $repo $LogPath
$logDir = Split-Path -Parent $logFullPath
if ($logDir -and -not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}

function Get-CommsSignature {
    param([string]$RelativePath)

    $fullPath = Join-Path $repo $RelativePath
    if (-not (Test-Path $fullPath)) {
        return "missing"
    }

    $item = Get-Item $fullPath
    return "$($item.LastWriteTimeUtc.Ticks):$($item.Length)"
}

function Write-CommsAlert {
    param([string]$Message)

    $stamp = Get-Date -Format o
    Add-Content -LiteralPath $logFullPath -Value "[$stamp] $Message"
}

$state = @{}
foreach ($path in $Paths) {
    $state[$path] = Get-CommsSignature $path
}

Write-CommsAlert "watcher started pid=$PID poll_seconds=$PollSeconds targets=$($Paths -join ', ')"

while ($true) {
    foreach ($path in $Paths) {
        $signature = Get-CommsSignature $path
        if ($state[$path] -ne $signature) {
            Write-CommsAlert "changed path=$path old=$($state[$path]) new=$signature"
            $state[$path] = $signature
        }
    }

    Start-Sleep -Seconds $PollSeconds
}
