[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("status", "run", "stop")]
    [string]$Command,

    [Parameter()]
    [string]$Task,

    [Parameter()]
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if ($Command -eq "run" -and [string]::IsNullOrWhiteSpace($Task)) {
    throw "run requires -Task <TASK.json>"
}

if ($Command -ne "run" -and ($PSBoundParameters.ContainsKey("Task") -or $DryRun)) {
    throw "-Task and -DryRun are valid only with the run command"
}

$pythonArgs = @("-m", "src.brain.cli", $Command)
if ($Command -eq "run") {
    $resolvedTask = (Resolve-Path -LiteralPath $Task).Path
    $pythonArgs += @("--task", $resolvedTask)
    if ($DryRun) {
        $pythonArgs += "--dry-run"
    }
}

Push-Location -LiteralPath $PSScriptRoot
try {
    & python @pythonArgs
    $pythonExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
exit $pythonExitCode
