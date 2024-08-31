$ErrorActionPreference = "Stop"
$ProgressPreference = 'SilentlyContinue'
          
# Configure private token (Windows)
$url = ("https://" + $env:GITHUB_TOKEN + ":x-oauth-basic@github.com/")
git config --global url."$url".insteadOf "https://github.com/"

# Check if MC is already available
if (!(Get-Command mc.exe -ErrorAction SilentlyContinue)) {
    Write-Output "MC not found in path. Downloading..."
    Invoke-WebRequest -Uri "https://dl.min.io/client/mc/release/windows-amd64/mc.exe" -OutFile "mc.exe"
    $env:PATH += ";$PWD"
} else {
    Write-Output "MC already in path"
}

# Determine the architecture
$ARCH = (Get-WmiObject Win32_Processor).Architecture

if ($ARCH -eq 9) {
    Write-Host "Detected ARM64 architecture"
    mc cp R2/sling/bin/sling_prep/sling_prep_windows_arm64.exe .
    .\sling_prep_windows_arm64.exe
} else {
    Write-Host "Detected x86_64 architecture"
    mc cp R2/sling/bin/sling_prep/sling_prep_windows_amd64.exe .
    .\sling_prep_windows_amd64.exe
}