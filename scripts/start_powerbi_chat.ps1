param(
    [ValidateRange(1, 65535)]
    [int]$Port = 8765
)

# Power BI Desktop's visual has an opaque browser origin. Code-free local mode
# is only for the project's public, aggregate CSV export on this PC.
$previousOrigins = [Environment]::GetEnvironmentVariable('CHAT_ALLOWED_ORIGINS', 'Process')
$previousAuthMode = [Environment]::GetEnvironmentVariable('CHAT_AUTH_MODE', 'Process')
$previousDataSource = [Environment]::GetEnvironmentVariable('CHAT_DATA_SOURCE', 'Process')
$previousCsvDir = [Environment]::GetEnvironmentVariable('CHAT_CSV_DIR', 'Process')
$previousModel = [Environment]::GetEnvironmentVariable('CHAT_OLLAMA_MODEL', 'Process')
$repositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path

try {
    $env:CHAT_ALLOWED_ORIGINS = 'https://ms-pbi.pbi.microsoft.com,null'
    $env:CHAT_AUTH_MODE = 'local_public'
    $env:CHAT_DATA_SOURCE = 'csv'
    $env:CHAT_CSV_DIR = Join-Path $repositoryRoot 'data\gold\powerbi'
    if (-not $previousModel) {
        try {
            $models = (Invoke-RestMethod -Uri 'http://127.0.0.1:11434/api/tags' -TimeoutSec 1).models
            if ($models | Where-Object { $_.name -eq 'qwen2.5:1.5b' }) {
                $env:CHAT_OLLAMA_MODEL = 'qwen2.5:1.5b'
            }
        }
        catch {
            # Ollama is optional; fixed data-backed answers still work.
        }
    }
    Push-Location -LiteralPath $repositoryRoot
    try {
        Write-Host 'Starting the local, read-only City Pulse chat service.'
        Write-Host 'No access code is needed. Keep this terminal open while using the report chat.'
        Write-Host 'Only exported aggregate taxi CSVs are available through this loopback service.'
        Write-Host ('Local AI question interpretation: ' + $(if ($env:CHAT_OLLAMA_MODEL) { 'configured' } else { 'unavailable; built-in answers remain available' }))
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
    if ($null -eq $previousOrigins) {
        Remove-Item Env:CHAT_ALLOWED_ORIGINS -ErrorAction SilentlyContinue
    }
    else {
        $env:CHAT_ALLOWED_ORIGINS = $previousOrigins
    }
    if ($null -eq $previousAuthMode) {
        Remove-Item Env:CHAT_AUTH_MODE -ErrorAction SilentlyContinue
    }
    else {
        $env:CHAT_AUTH_MODE = $previousAuthMode
    }
    if ($null -eq $previousDataSource) {
        Remove-Item Env:CHAT_DATA_SOURCE -ErrorAction SilentlyContinue
    }
    else {
        $env:CHAT_DATA_SOURCE = $previousDataSource
    }
    if ($null -eq $previousCsvDir) {
        Remove-Item Env:CHAT_CSV_DIR -ErrorAction SilentlyContinue
    }
    else {
        $env:CHAT_CSV_DIR = $previousCsvDir
    }
    if ($null -eq $previousModel) {
        Remove-Item Env:CHAT_OLLAMA_MODEL -ErrorAction SilentlyContinue
    }
    else {
        $env:CHAT_OLLAMA_MODEL = $previousModel
    }
}
