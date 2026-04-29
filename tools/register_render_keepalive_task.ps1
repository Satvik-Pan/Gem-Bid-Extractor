param(
    [string]$TaskName = "GemBidDashboardKeepAlive",
    [int]$IntervalMinutes = 10
)

$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$scriptPath = Join-Path $repo "tools\keep_render_awake.ps1"

if (-not (Test-Path $scriptPath)) {
    throw "keep_render_awake.ps1 not found at $scriptPath"
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Output "Scheduled keepalive task '$TaskName' created/updated for every $IntervalMinutes minutes."
Write-Output "Verify with: Get-ScheduledTask -TaskName $TaskName | Format-List"
