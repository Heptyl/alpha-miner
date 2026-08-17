param(
    [string]$UvCommand = 'uv',
    [string]$ProjectRoot = '',
    [string]$LogFile = ''
)

$ErrorActionPreference = 'Stop'
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$StrictUtf8 = New-Object System.Text.UTF8Encoding($false, $true)

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
} else {
    $ProjectRoot = (Resolve-Path $ProjectRoot).Path
}
if ([string]::IsNullOrWhiteSpace($LogFile)) {
    $LogFile = Join-Path $ProjectRoot ("logs\limit_up_collection_{0}.log" -f (Get-Date -Format 'yyyyMMdd'))
} else {
    $LogFile = [System.IO.Path]::GetFullPath($LogFile)
}
$LogDir = Split-Path -Parent $LogFile
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

function Initialize-Utf8Log {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }
    $Bytes = [System.IO.File]::ReadAllBytes($Path)
    if ($Bytes.Length -ge 2 -and $Bytes[0] -eq 0xFF -and $Bytes[1] -eq 0xFE) {
        $Text = [System.Text.Encoding]::Unicode.GetString($Bytes, 2, $Bytes.Length - 2)
        [System.IO.File]::WriteAllText($Path, $Text, $Utf8NoBom)
        return
    }
    if ($Bytes.Length -ge 2 -and $Bytes[0] -eq 0xFE -and $Bytes[1] -eq 0xFF) {
        $Text = [System.Text.Encoding]::BigEndianUnicode.GetString($Bytes, 2, $Bytes.Length - 2)
        [System.IO.File]::WriteAllText($Path, $Text, $Utf8NoBom)
        return
    }
    if ($Bytes.Length -ge 3 -and $Bytes[0] -eq 0xEF -and $Bytes[1] -eq 0xBB -and $Bytes[2] -eq 0xBF) {
        $Text = [System.Text.Encoding]::UTF8.GetString($Bytes, 3, $Bytes.Length - 3)
        [System.IO.File]::WriteAllText($Path, $Text, $Utf8NoBom)
        return
    }
    try {
        $null = $StrictUtf8.GetString($Bytes)
    } catch {
        throw "Existing log is not valid UTF-8: $Path"
    }
}

function Invoke-NativeLogged {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )

    $PreviousPreference = $ErrorActionPreference
    $PreviousConsoleEncoding = [Console]::OutputEncoding
    $NativeExit = $null
    try {
        # Windows PowerShell 5.1 turns redirected native stderr into ErrorRecord.
        # Keep it in the merged stream without letting a successful native process
        # trip the script-wide Stop preference before LASTEXITCODE is captured.
        [Console]::OutputEncoding = $Utf8NoBom
        $ErrorActionPreference = 'Continue'
        & $FilePath @Arguments 2>&1 | ForEach-Object {
            try {
                [System.IO.File]::AppendAllText(
                    $LogFile,
                    $_.ToString() + [Environment]::NewLine,
                    $Utf8NoBom
                )
            } catch {
                throw
            }
        }
        $NativeExit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousPreference
        [Console]::OutputEncoding = $PreviousConsoleEncoding
    }
    if ($null -eq $NativeExit) {
        throw "Native command did not provide an exit code: $FilePath"
    }
    return [int]$NativeExit
}

Initialize-Utf8Log -Path $LogFile
$UvCommand = (Get-Command $UvCommand -ErrorAction Stop).Source
Set-Location $ProjectRoot
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'

$CollectExit = Invoke-NativeLogged -FilePath $UvCommand -Arguments @(
    'run', 'python', '-m', 'cli', 'zt', 'collect'
)
if ($CollectExit -ne 0) {
    exit $CollectExit
}

$StatusExit = Invoke-NativeLogged -FilePath $UvCommand -Arguments @(
    'run', 'python', '-m', 'cli', 'zt', 'status', '--strict'
)
exit $StatusExit
