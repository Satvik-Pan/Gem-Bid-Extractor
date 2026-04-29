param(
    [string]$Url = "https://gem-bid-extractor.onrender.com/api/health"
)

$ErrorActionPreference = "Stop"

try {
    $response = Invoke-WebRequest -Uri $Url -Method Get -TimeoutSec 20 -UseBasicParsing
    if ($response.StatusCode -lt 200 -or $response.StatusCode -ge 300) {
        throw "Unexpected status code: $($response.StatusCode)"
    }
    Write-Output "Keepalive success: $Url [$($response.StatusCode)]"
} catch {
    Write-Error "Keepalive failed: $($_.Exception.Message)"
    exit 1
}
