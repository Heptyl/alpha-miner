# Copy this file to remote.local.ps1 and fill in private deployment values.
# remote.local.ps1 is ignored by Git and must never be committed.
$env:ALPHA_MINER_SSH_TARGET = '<ssh-user>@<server-ip>'
$env:ALPHA_MINER_REMOTE_ROOT = '/path/to/alpha-miner'
