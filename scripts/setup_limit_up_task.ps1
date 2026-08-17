param(
    [ValidateSet('show', 'install', 'remove')]
    [string]$Action = 'show',
    [string]$At = '16:10',
    [string]$RetryAt = '18:10'
)

$ErrorActionPreference = 'Stop'
$TaskName = 'AlphaMiner-LimitUpHistory'
$Runner = (Resolve-Path (Join-Path $PSScriptRoot 'run_limit_up_collection.ps1')).Path

if ($Action -eq 'show') {
    $Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -eq $Task) {
        Write-Host "$TaskName is not installed."
        exit 1
    }
    $Task | Format-List TaskName, State, Description
    Get-ScheduledTaskInfo -TaskName $TaskName |
        Format-List LastRunTime, LastTaskResult, NextRunTime
    exit 0
}

if ($Action -eq 'remove') {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Removed $TaskName"
    exit 0
}

$PowerShell = (Get-Command powershell.exe).Source
$UvCommand = (Get-Command uv).Source
$Arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}" -UvCommand "{1}"' -f $Runner, $UvCommand
$TaskAction = New-ScheduledTaskAction -Execute $PowerShell -Argument $Arguments
$Weekdays = @('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
$Triggers = @(
    New-ScheduledTaskTrigger -Weekly -DaysOfWeek $Weekdays -At $At
    New-ScheduledTaskTrigger -Weekly -DaysOfWeek $Weekdays -At $RetryAt
)
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 1)
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $TaskAction `
    -Trigger $Triggers `
    -Settings $Settings `
    -Description 'Collect, audit, and display Alpha Miner limit-up history.' `
    -Force | Out-Null
Write-Host "Installed $TaskName at $At and $RetryAt on weekdays."
