# PowerShell script to build a Windows executable using pyinstaller
# Usage: run in project root on Windows (PowerShell) after creating and activating venv:
#   pip install -r requirements.txt -r requirements-win.txt
#   .\.venv\Scripts\activate
#   ./build_windows.ps1

param(
    [string]$Entry='app.py',
    [string]$Name='tiktok_danmu',
    [string]$Icon=''  # optional .ico path
)

function Get-BuildPython {
    $candidates = @(
        ".\.venv\Scripts\python.exe",
        ".\.venv-build\Scripts\python.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return (Resolve-Path $candidate).Path
        }
    }
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        return "py"
    }
    return "python"
}

$pythonCmd = Get-BuildPython
$pythonArgs = @()
if ($pythonCmd -eq "py") {
    $pythonArgs = @("-3.10")
}

Write-Host "Building $Entry -> $Name.exe"
Write-Host "Using Python: $pythonCmd $($pythonArgs -join ' ')"

& $pythonCmd @pythonArgs -m pip install pyinstaller --quiet
if ($LASTEXITCODE -ne 0) {
    throw "Failed to install PyInstaller."
}

$buildArgs = @("-m", "PyInstaller", "--onefile", "--noconsole", "--name", $Name, $Entry)
if ($Icon -ne '') {
    $buildArgs += @("--icon", $Icon)
}

Write-Host "Running: $pythonCmd $($pythonArgs + $buildArgs -join ' ')"
& $pythonCmd @pythonArgs @buildArgs
if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller build failed."
}

Write-Host "Build finished. See dist\$Name.exe"
