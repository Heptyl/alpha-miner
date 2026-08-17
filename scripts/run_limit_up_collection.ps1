param(
    [string]$UvCommand = 'uv'
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$LogDir = Join-Path $ProjectRoot 'logs'
$LogFile = Join-Path $LogDir ("limit_up_collection_{0}.log" -f (Get-Date -Format 'yyyyMMdd'))
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
Set-Location $ProjectRoot
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'

& $UvCommand run python -m cli zt collect 2>&1 |
    Tee-Object -FilePath $LogFile -Append
$CollectExit = $LASTEXITCODE
if ($CollectExit -ne 0) {
    exit $CollectExit
}

& $UvCommand run python -m cli zt status --strict 2>&1 |
    Tee-Object -FilePath $LogFile -Append
exit $LASTEXITCODE
