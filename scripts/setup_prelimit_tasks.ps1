param(
    [ValidateSet('show', 'definition', 'install', 'remove')]
    [string]$Action = 'show',
    [string]$AuctionAt = '09:26',
    [string]$OpenAt = '09:31'
)

$ErrorActionPreference = 'Stop'
$Runner = (Resolve-Path (Join-Path $PSScriptRoot 'run_prelimit_capture.ps1')).Path
$Definitions = @(
    [pscustomobject]@{
        TaskName = 'AlphaMiner-Prelimit-Auction0925'
        Phase = 'auction'
        At = $AuctionAt
    }
    [pscustomobject]@{
        TaskName = 'AlphaMiner-Prelimit-Open0931'
        Phase = 'open'
        At = $OpenAt
    }
)

if ($Action -eq 'definition') {
    $Definitions | Format-Table TaskName, Phase, At
    exit 0
}

if ($Action -eq 'show') {
    foreach ($Definition in $Definitions) {
        $Task = Get-ScheduledTask -TaskName $Definition.TaskName -ErrorAction SilentlyContinue
        if ($null -eq $Task) {
            Write-Host "$($Definition.TaskName) is not installed."
            continue
        }
        $Task | Format-List TaskName, State, Description
        Get-ScheduledTaskInfo -TaskName $Definition.TaskName |
            Format-List LastRunTime, LastTaskResult, NextRunTime
    }
    exit 0
}

if ($Action -eq 'remove') {
    foreach ($Definition in $Definitions) {
        Unregister-ScheduledTask `
            -TaskName $Definition.TaskName `
            -Confirm:$false `
            -ErrorAction SilentlyContinue
    }
    Write-Host 'Removed Alpha Miner pre-limit tasks.'
    exit 0
}

$PowerShell = (Get-Command powershell.exe).Source
$UvCommand = (Get-Command uv).Source
$Weekdays = @('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
foreach ($Definition in $Definitions) {
    $Arguments = (
        '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}" ' +
        '-Phase "{1}" -UvCommand "{2}"'
    ) -f $Runner, $Definition.Phase, $UvCommand
    $TaskAction = New-ScheduledTaskAction -Execute $PowerShell -Argument $Arguments
    $Trigger = New-ScheduledTaskTrigger `
        -Weekly `
        -DaysOfWeek $Weekdays `
        -At $Definition.At
    Register-ScheduledTask `
        -TaskName $Definition.TaskName `
        -Action $TaskAction `
        -Trigger $Trigger `
        -Settings $Settings `
        -Description "Capture $($Definition.Phase) forward pre-limit evidence." `
        -Force | Out-Null
}
Write-Host 'Installed Alpha Miner 09:26 and 09:31 pre-limit tasks.'
