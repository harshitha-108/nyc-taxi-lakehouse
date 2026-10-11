param(
    [ValidateRange(1, 65535)]
    [int]$Port = 8765
)

# Explicit opt-in: unlike the normal CSV launcher, this protected mode reads
# published PostgreSQL marts with a dedicated SELECT-only account.
$repositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$environmentPath = Join-Path $repositoryRoot '.env'
if (-not (Test-Path -LiteralPath $environmentPath -PathType Leaf)) {
    throw 'Create a local .env with CHAT_READONLY_DB_USER/PASSWORD first.'
}
$keys = @(
    'ANALYTICS_DB_NAME', 'CHAT_READONLY_DB_USER', 'CHAT_READONLY_DB_PASSWORD',
    'CHAT_READONLY_DB_HOST', 'CHAT_READONLY_DB_PORT', 'CHAT_READONLY_DB_NAME',
    'CHAT_OLLAMA_MODEL'
)
$previous = @{}
foreach ($key in $keys + @('CHAT_AUTH_MODE', 'CHAT_DATA_SOURCE',
                          'CHAT_ENABLE_DYNAMIC_SQL', 'CHAT_ALLOWED_ORIGINS')) {
    $previous[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
}
try {
    foreach ($line in Get-Content -LiteralPath $environmentPath) {
        if ($line -match '^([A-Z][A-Z0-9_]*)=(.*)$' -and $keys -contains $Matches[1]) {
            [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], 'Process')
        }
    }
    if (-not $env:CHAT_READONLY_DB_USER -or -not $env:CHAT_READONLY_DB_PASSWORD) {
        throw 'Both CHAT_READONLY_DB_USER and CHAT_READONLY_DB_PASSWORD are required in .env.'
    }
    if ($env:CHAT_READONLY_DB_HOST -ne '127.0.0.1' -or
        $env:CHAT_READONLY_DB_PORT -ne '5433') {
        throw 'Use CHAT_READONLY_DB_HOST=127.0.0.1 and CHAT_READONLY_DB_PORT=5433.'
    }
    $env:CHAT_AUTH_MODE = 'code'
    $env:CHAT_DATA_SOURCE = 'postgres'
    $env:CHAT_ENABLE_DYNAMIC_SQL = '1'
    $env:CHAT_ALLOWED_ORIGINS = 'https://ms-pbi.pbi.microsoft.com,null'
    Push-Location -LiteralPath $repositoryRoot
    try {
        Write-Host "Starting protected City Pulse SQL mode on local port $Port."
        Write-Host 'Use the temporary access code printed by the service in the chat popup.'
        Write-Host 'Only approved published aggregates are queryable; no trip-level tables.'
        & python -m chatbot.server --port $Port
        if ($LASTEXITCODE -ne 0) {
            throw "Chat service exited with code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}
finally {
    foreach ($key in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($key, $previous[$key], 'Process')
    }
}
