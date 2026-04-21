param(
    [string]$TaskName = "GemBidExtractorDaily",
    [string]$RunAt = "12:00"
)

$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$bat = Join-Path $repo "run_extractor.bat"

if (-not (Test-Path $bat)) {
    throw "run_extractor.bat not found at $bat"
}

# Create the scheduled task action - run the batch file via cmd
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$bat`"" -WorkingDirectory $repo

# Daily trigger at the specified time
$trigger = New-ScheduledTaskTrigger -Daily -At $RunAt

# Run whether user is logged on or not is not needed - Interactive is fine for desktop use
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

# Settings: start even on battery, don't stop if switching to battery, start if missed
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Output "Scheduled task '$TaskName' created/updated to run daily at $RunAt"
Write-Output "Verify with: Get-ScheduledTask -TaskName $TaskName | Format-List"
