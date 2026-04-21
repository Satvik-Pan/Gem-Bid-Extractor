param(
    [string]$RepoPath = ""
)

$ErrorActionPreference = "Stop"

if (-not $RepoPath) {
    $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

Set-Location $RepoPath

# Verify origin points to GitHub
$originUrl = git remote get-url origin 2>$null
if (-not $originUrl -or $originUrl -notmatch "github\.com") {
    Write-Warning "[auto-git] Origin URL is not GitHub ($originUrl). Skipping push."
    exit 0
}

# Check for uncommitted changes
$status = git status --porcelain
if (-not $status) {
    Write-Output "[auto-git] No changes to commit."
    exit 0
}

# Stage, commit, and push to GitHub
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
git add -A
$msg = "Auto update after extractor run ($timestamp)"
git commit -m $msg | Out-Null

try {
    git push origin main 2>&1 | Out-Null
    Write-Output "[auto-git] Pushed to GitHub origin/main successfully."
} catch {
    Write-Warning "[auto-git] Push to GitHub failed: $($_.Exception.Message)"
    exit 0
}
