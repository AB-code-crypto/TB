$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

$Python = ".\.venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    throw "Виртуальное окружение не найдено: $Python"
}

Write-Host "== Compile Python files ==" -ForegroundColor Cyan
& $Python -m compileall gui tbank bd bot

Write-Host "== Install / update PyInstaller ==" -ForegroundColor Cyan
& $Python -m pip install --upgrade pyinstaller

Write-Host "== Clean old build ==" -ForegroundColor Cyan

if (Test-Path ".\build") {
    Remove-Item ".\build" -Recurse -Force
}

if (Test-Path ".\dist") {
    Remove-Item ".\dist" -Recurse -Force
}

if (Test-Path ".\TBankRobot.spec") {
    Remove-Item ".\TBankRobot.spec" -Force
}

Write-Host "== Build client onedir exe ==" -ForegroundColor Cyan
& $Python -m PyInstaller `
    --noconfirm `
    --clean `
    --onedir `
    --windowed `
    --noupx `
    --name "TBankRobot" `
    --collect-all "PySide6" `
    --collect-all "t_tech" `
    --collect-all "grpc" `
    --collect-all "google.protobuf" `
    ".\main_gui.py"

New-Item -ItemType Directory -Force ".\dist\TBankRobot\data" | Out-Null

Write-Host ""
Write-Host "Build completed:" -ForegroundColor Green
Write-Host "  .\dist\TBankRobot\TBankRobot.exe"
Write-Host ""
Write-Host "For client delivery, zip the whole folder:" -ForegroundColor Yellow
Write-Host "  .\dist\TBankRobot"
Write-Host ""
Write-Host "Do NOT copy your local data\tbank_robot.sqlite3 or .env to the client build." -ForegroundColor Yellow
