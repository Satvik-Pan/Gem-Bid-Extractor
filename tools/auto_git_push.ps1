param(
    [string]$RepoPath = ""
)

$ErrorActionPreference = "Stop"

if (-not $RepoPath) {
    $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

Set-Location $RepoPath

$originFetch = git remote get-url origin
if (-not $originFetch -or $originFetch -notmatch "github.com") {
    Write-Warning "[auto-git] Origin fetch URL is not GitHub. Skipping push."
    exit 0
}

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
    # Push directly to the GitHub fetch URL to avoid any extra configured push URLs.
    git push $originFetch main | Out-Null
    Write-Output "[auto-git] Pushed to origin/main successfully."
} catch {
    Write-Warning "[auto-git] Push failed: $($_.Exception.Message)"
    exit 0
}
