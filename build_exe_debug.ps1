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

Write-Host "== Clean old debug build ==" -ForegroundColor Cyan

if (Test-Path ".\build") {
    Remove-Item ".\build" -Recurse -Force
}

if (Test-Path ".\dist") {
    Remove-Item ".\dist" -Recurse -Force
}

if (Test-Path ".\TBankRobotDebug.spec") {
    Remove-Item ".\TBankRobotDebug.spec" -Force
}

Write-Host "== Build debug onedir exe with console ==" -ForegroundColor Cyan
& $Python -m PyInstaller `
    --noconfirm `
    --clean `
    --onedir `
    --console `
    --noupx `
    --name "TBankRobotDebug" `
    --collect-all "PySide6" `
    --collect-all "t_tech" `
    --collect-all "grpc" `
    --collect-all "google.protobuf" `
    ".\main_gui.py"

New-Item -ItemType Directory -Force ".\dist\TBankRobotDebug\data" | Out-Null

Write-Host ""
Write-Host "Debug build completed:" -ForegroundColor Green
Write-Host "  .\dist\TBankRobotDebug\TBankRobotDebug.exe"
Write-Host ""
Write-Host "Run it from PowerShell if the client build starts and closes silently."
