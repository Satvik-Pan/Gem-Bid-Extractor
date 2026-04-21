param(
    [string]$RepoPath = ""
)

$ErrorActionPreference = "Stop"

if (-not $RepoPath) {
    $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

Set-Location $RepoPath

$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$status = git status --porcelain
if (-not $status) {
    Write-Output "[auto-git] No changes to commit."
    exit 0
}

git add -A
$msg = "Auto update after extractor run ($timestamp)"
git commit -m $msg | Out-Null

try {
    git push origin main | Out-Null
    Write-Output "[auto-git] Pushed to origin/main successfully."
} catch {
    Write-Warning "[auto-git] Push failed: $($_.Exception.Message)"
    exit 0
}
