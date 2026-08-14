param(
    [ValidateSet('sync', 'build', 'collect', 'evolve', 'daily', 'snapshot', 'publish-data', 'status')]
    [string]$Action = 'status',
    [string]$SshTarget = 'leigeng@192.168.21.67',
    [string]$MappedRoot = 'X:\alpha-miner',
    [string]$RemoteRoot = '/home/diskc/leigeng/alpha-miner',
    [string]$LocalDb = 'data\alpha_miner.db',
    [switch]$SeedData
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$MappedParent = Split-Path -Parent $MappedRoot
$ResolvedParent = (Resolve-Path $MappedParent).Path
if (-not $MappedRoot.StartsWith($ResolvedParent, [StringComparison]::OrdinalIgnoreCase)) {
    throw "MappedRoot must stay under the resolved mapped drive: $ResolvedParent"
}

function Sync-Code {
    New-Item -ItemType Directory -Path $MappedRoot -Force | Out-Null
    $excludeDirs = @(
        '.git', '.venv', '.pytest_cache', '.uvtmp', '.uv_tmp',
        '.server-runtime', '__pycache__', 'logs', 'reports', 'signals', 'recommendations'
    )
    if (-not $SeedData) {
        $excludeDirs += 'data'
    }
    $copyArgs = @($ProjectRoot, $MappedRoot, '/E', '/R:2', '/W:1', '/NFL', '/NDL', '/NJH', '/NJS', '/NP', '/XD')
    $copyArgs += $excludeDirs
    & robocopy @copyArgs
    if ($LASTEXITCODE -ge 8) {
        throw "robocopy failed with exit code $LASTEXITCODE"
    }
}

function Prepare-OfflineRuntime {
    $runtimeRoot = Join-Path $MappedRoot '.server-runtime'
    New-Item -ItemType Directory -Path $runtimeRoot -Force | Out-Null

    $release = Invoke-RestMethod -UseBasicParsing -TimeoutSec 60 `
        -Headers @{ 'User-Agent' = 'alpha-miner-deployer' } `
        'https://api.github.com/repos/astral-sh/python-build-standalone/releases/latest'
    $asset = $release.assets | Where-Object {
        $_.name -match '^cpython-3\.12.*x86_64-unknown-linux-gnu-install_only_stripped\.tar\.gz$'
    } | Select-Object -First 1
    if (-not $asset) {
        throw 'Could not find a CPython 3.12 Linux standalone archive.'
    }

    $archive = Join-Path $runtimeRoot $asset.name
    if (-not (Test-Path $archive)) {
        Write-Host "Downloading $($asset.name)..."
        Invoke-WebRequest -UseBasicParsing -TimeoutSec 900 `
            -Headers @{ 'User-Agent' = 'alpha-miner-deployer' } `
            -Uri $asset.browser_download_url -OutFile $archive
    }

    $requirements = Join-Path $runtimeRoot 'requirements-linux.txt'
    & uv export --frozen --no-emit-project --no-hashes `
        --format requirements-txt --output-file $requirements --project $ProjectRoot
    if ($LASTEXITCODE -ne 0) {
        throw "uv export failed with exit code $LASTEXITCODE"
    }

    # Ubuntu 18.04 has glibc 2.27. The newest numerical wheels in the lock file
    # target newer glibc, so use the newest project-compatible manylinux2014 set.
    $overrides = Join-Path $runtimeRoot 'overrides-linux.txt'
    Set-Content -Path $overrides -Encoding ascii -Value @(
        'numpy==1.26.4',
        'pandas==2.2.3',
        'scipy==1.14.1'
    )

    $lockHash = (Get-FileHash (Join-Path $ProjectRoot 'uv.lock') -Algorithm SHA256).Hash.ToLowerInvariant()
    $siteName = "site-packages-$($lockHash.Substring(0, 12))"
    $sitePath = Join-Path $runtimeRoot $siteName
    $completeMarker = Join-Path $sitePath '.complete'
    if (-not (Test-Path $completeMarker)) {
        Write-Host "Preparing Linux dependencies in $siteName..."
        $env:UV_LINK_MODE = 'copy'
        & uv pip install --target $sitePath `
            --python-platform x86_64-manylinux_2_17 `
            --python-version 3.12 --overrides $overrides --requirements $requirements
        if ($LASTEXITCODE -ne 0) {
            throw "uv pip install failed with exit code $LASTEXITCODE"
        }
        New-Item -ItemType File -Path $completeMarker -Force | Out-Null
    }
    Set-Content -Path (Join-Path $runtimeRoot 'active-site.txt') `
        -Value $siteName -Encoding ascii
}

if ($Action -eq 'sync') {
    Sync-Code
    Write-Host "Synced to $MappedRoot"
    exit 0
}

if (-not (Test-Path (Join-Path $MappedRoot 'scripts\server_run.sh'))) {
    Sync-Code
}

if ($Action -eq 'build') {
    Sync-Code
    Prepare-OfflineRuntime
}

if ($Action -eq 'publish-data') {
    Sync-Code
    $sourceDb = Join-Path $ProjectRoot $LocalDb
    $targetDb = Join-Path $MappedRoot 'incoming\alpha_miner.db'
    & (Join-Path $ProjectRoot '.venv\Scripts\python.exe') `
        (Join-Path $ProjectRoot 'scripts\publish_data.py') `
        --source $sourceDb --target $targetDb
    if ($LASTEXITCODE -ne 0) {
        throw "database publish failed with exit code $LASTEXITCODE"
    }
}

$remoteAction = switch ($Action) {
    'build' { 'build' }
    'collect' { 'collect' }
    'evolve' { 'evolve' }
    'daily' { 'daily' }
    'snapshot' { 'snapshot' }
    'publish-data' { 'activate-data' }
    default { 'status' }
}

ssh $SshTarget "cd '$RemoteRoot' && bash scripts/server_run.sh $remoteAction"
if ($LASTEXITCODE -ne 0) {
    throw "remote command failed with exit code $LASTEXITCODE"
}
