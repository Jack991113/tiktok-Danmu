param(
    [string]$ExeName = "tiktok_danmu_public"
)

$ErrorActionPreference = "Stop"

$dateStamp = Get-Date -Format "yyyyMMdd"
$bundleRoot = Join-Path $PSScriptRoot ("release\\tiktok_danmu_public_bundle_" + $dateStamp)
$appsDir = Join-Path $bundleRoot "apps"
$docsDir = Join-Path $bundleRoot "docs"
$zipPath = $bundleRoot + ".zip"

if (Test-Path $bundleRoot) {
    Remove-Item -Recurse -Force $bundleRoot
}
if (Test-Path $zipPath) {
    Remove-Item -Force $zipPath
}

New-Item -ItemType Directory -Force -Path $appsDir | Out-Null
New-Item -ItemType Directory -Force -Path $docsDir | Out-Null

powershell -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "build_windows.ps1") -Entry "app.py" -Name $ExeName

$exePath = Join-Path $PSScriptRoot ("dist\\" + $ExeName + ".exe")
if (-not (Test-Path $exePath)) {
    throw "Build output not found: $exePath"
}

Copy-Item $exePath (Join-Path $appsDir ($ExeName + ".exe")) -Force
Copy-Item (Join-Path $PSScriptRoot "README.md") (Join-Path $docsDir "README.md") -Force
Copy-Item (Join-Path $PSScriptRoot "CONFIG_GUIDE.txt") (Join-Path $docsDir "CONFIG_GUIDE.txt") -Force

Compress-Archive -Path $bundleRoot -DestinationPath $zipPath -Force

Write-Host "Public bundle created:"
Write-Host $bundleRoot
Write-Host $zipPath
