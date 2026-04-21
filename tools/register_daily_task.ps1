param(
    [string]$TaskName = "GemBidExtractorDaily",
    [string]$RunAt = "11:00"
)

$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$bat = Join-Path $repo "run_extractor.bat"

if (-not (Test-Path $bat)) {
    throw "run_extractor.bat not found at $bat"
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c \"$bat\""
$trigger = New-ScheduledTaskTrigger -Daily -At $RunAt
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Output "Scheduled task '$TaskName' created/updated to run daily at $RunAt"
