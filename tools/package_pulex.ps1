<#
.SYNOPSIS
    Dedicated Production Packaging Script for Pulex v3.0.0 (Tauri v2 Edition)
    Standard: HelloKittyXamKinLung Indie Release Pipeline

.DESCRIPTION
    1. Validates and compiles Vite 6 frontend bundle (`npm run build` unless -SkipFrontendBuild is set).
    2. Compiles native Rust backend (`cargo build --release --locked`).
    3. Verifies footprint gate (< 75 MB uncompressed; DuckDB 1.2.2 + WebView2Loader + Pulex).
    4. Packages ONLY `Pulex-<version>-x64-portable.exe` and `Pulex-<version>-x64-portable.zip`
       (zero duplicate binaries inside ZIP or across historical versions).
    5. Generates Winget 1.6.0 `HelloKittyXamKinLung.Pulex.yaml` (zip + nested portable) and `SHA256SUMS.txt`.
    6. Deploys canonical local development executable strictly to `<root>/pulex.exe`.
#>

param (
    [string]$AppName = "Pulex",
    [string]$Version = "3.0.4",
    [string]$Publisher = "HelloKittyXamKinLung",
    [switch]$SkipFrontendBuild = $false,
    [switch]$SkipBackendBuild = $false
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$projectRoot = Split-Path $PSScriptRoot -Parent
$node22Dir = "$projectRoot\tools\node22"
$gitPath = "$projectRoot\mingit\cmd"
$jjPath = "$projectRoot\tools\jj"
$cargoPath = "$env:USERPROFILE\.cargo\bin"
$env:PATH = "$node22Dir;$gitPath;$jjPath;$cargoPath;$env:PATH"

$srcTauriDir = "$projectRoot\src-tauri"
$targetReleaseDir = "$srcTauriDir\target\release"
$releasesDir = "$projectRoot\releases"

Write-Output "======================================================================"
Write-Output "  PULEX TAURI v2 PRODUCTION PACKAGING PIPELINE"
Write-Output "  App: $AppName | Version: $Version | Publisher: $Publisher"
Write-Output "======================================================================"

# 1. Build Vite Frontend Production Bundle
Set-Location $projectRoot
if (-not $SkipFrontendBuild) {
    Write-Output "[STEP 1/6] Building Vite Frontend Production Bundle..."
    $prevEAP = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & node tools/patch_slickgrid_stylesheet.cjs
    if ($LASTEXITCODE -ne 0) { throw "patch_slickgrid_stylesheet.cjs failed" }
    & node .\node_modules\typescript\bin\tsc
    if ($LASTEXITCODE -ne 0) { throw "TypeScript compilation failed" }
    & node .\node_modules\vite\bin\vite.js build
    if ($LASTEXITCODE -ne 0) { throw "Vite production build failed" }
    $ErrorActionPreference = $prevEAP
} else {
    Write-Output "[STEP 1/6] Using pre-built Frontend Production Bundle in dist/..."
}

if (-not (Test-Path "$projectRoot\dist\index.html")) {
    throw "ERROR: Frontend dist/index.html was not generated!"
}
Write-Output "           Frontend bundle verified in dist/"

# 2. Compile Native Rust Backend
Set-Location $srcTauriDir
if (-not $SkipBackendBuild) {
    Write-Output "[STEP 2/6] Compiling Native Rust Core with cargo build --release --locked..."
    $outLog = Join-Path $env:TEMP "pulex_cargo_release_out.log"
    $errLog = Join-Path $env:TEMP "pulex_cargo_release_err.log"
    $proc = Start-Process -FilePath "$cargoPath\cargo.exe" -ArgumentList @("build", "--release", "--locked") -WorkingDirectory $srcTauriDir -RedirectStandardOutput $outLog -RedirectStandardError $errLog -NoNewWindow -PassThru -Wait
    if ($proc.ExitCode -ne 0) {
        if (Test-Path $errLog) { Get-Content $errLog -Tail 40 | Write-Output }
        throw "ERROR: cargo build --release --locked failed with exit code $($proc.ExitCode)!"
    }
} else {
    Write-Output "[STEP 2/6] Using pre-built Native Rust Core in target/release/pulex.exe..."
}

$builtExe = "$targetReleaseDir\pulex.exe"
if (-not (Test-Path $builtExe)) {
    throw "ERROR: $builtExe not found after release build!"
}
if (-not (Test-Path "$targetReleaseDir\duckdb.dll")) {
    throw "ERROR: duckdb.dll missing from target/release!"
}
if (-not (Test-Path "$targetReleaseDir\WebView2Loader.dll")) {
    throw "ERROR: WebView2Loader.dll missing from target/release!"
}

# Prune any stale legacy toad.* artifacts inside target/release
Get-ChildItem -Path $targetReleaseDir -Filter "*toad*" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -Confirm:$false -ErrorAction SilentlyContinue
Write-Output "           Backend binary ($builtExe) and companion DLLs verified."

# 3. Footprint Gate Assertion (Single-File Self-Contained Executable)
Write-Output "[STEP 3/6] Verifying Footprint Gate..."
$exeItem = Get-Item $builtExe
$exeSizeGateMB = [math]::Round($exeItem.Length / 1MB, 2)
Write-Output "           Pulex Single-File Portable Binary (with embedded gzip DuckDB 1.2.2 + WebView2Loader): $exeSizeGateMB MB"
if ($exeSizeGateMB -ge 55.0) {
    throw "FOOTPRINT GATE ERROR: Single-file executable ($exeSizeGateMB MB) exceeds 55.0 MB ceiling!"
}

# 4. Enforce Zero-Duplicate Clean Releases Directory
Write-Output "[STEP 4/6] Enforcing Clean Zero-Duplicate Releases Directory..."
if (-not (Test-Path $releasesDir)) {
    New-Item -ItemType Directory -Path $releasesDir -Force | Out-Null
}

$pExeName = "Pulex-$Version-x64-portable.exe"
$pZipName = "Pulex-$Version-x64-portable.zip"

Get-ChildItem -Path $releasesDir -File -ErrorAction SilentlyContinue | ForEach-Object {
    $n = $_.Name
    if (
        $n -like "*Toad*" -or
        $n -like "*-beta*" -or
        $n -like "*.dll" -or
        $n -like "v2.*_NOTES.md" -or
        ($n -like "Pulex-*-portable.exe" -and $n -ne $pExeName) -or
        ($n -like "Pulex-*-portable.zip" -and $n -ne $pZipName)
    ) {
        Write-Output "           Pruning historical/duplicate artifact: $n"
        Remove-Item $_.FullName -Force
    }
}

# 5. Package Canonical Pulex Release Artifacts
Write-Output "[STEP 5/6] Packaging Canonical Pulex Release Artifacts..."
$sDir = Join-Path $env:TEMP "Pulex_package_stage_$Version"
if (Test-Path $sDir) { Remove-Item -Recurse -Force $sDir }
New-Item -ItemType Directory -Path $sDir | Out-Null

# Strict Single-File Portable ZIP layout: ONLY Pulex-<version>-x64-portable.exe (zero external DLLs needed)
Copy-Item $builtExe "$sDir\$pExeName"

$zPulex = Join-Path $releasesDir $pZipName
if (Test-Path $zPulex) { Remove-Item $zPulex -Force }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory(
    $sDir,
    $zPulex,
    [System.IO.Compression.CompressionLevel]::Optimal,
    $false
)

$ePulex = Join-Path $releasesDir $pExeName
Copy-Item $builtExe $ePulex -Force

# Deploy single canonical development/test executable at workspace root
Copy-Item $builtExe "$projectRoot\pulex.exe" -Force

Remove-Item -Recurse -Force $sDir -ErrorAction SilentlyContinue

# Winget 1.6.0 Manifest (zip + nested portable so companion DLLs stay side-by-side)
$zipHash = (Get-FileHash -Path $zPulex -Algorithm SHA256).Hash.ToUpper()
$exeHash = (Get-FileHash -Path $ePulex -Algorithm SHA256).Hash.ToUpper()

$wContent = @"
PackageIdentifier: $Publisher.Pulex
PackageVersion: $Version
PackageLocale: en-US
Publisher: $Publisher
PackageName: Pulex
License: MIT
ShortDescription: High-Performance Tabular Data & Security Forensic Log Analyzer (Tauri v2 + DuckDB 1.2.2)
Tags:
  - csv
  - parquet
  - duckdb
  - evtx
  - security
  - forensics
Installers:
  - Architecture: x64
    InstallerType: zip
    NestedInstallerType: portable
    NestedInstallerFiles:
      - RelativeFilePath: $pExeName
        PortableCommandAlias: pulex
    InstallerUrl: https://github.com/p1rkr41n/pulex/releases/download/v$Version/$pZipName
    InstallerSha256: $zipHash
ManifestType: singleton
ManifestVersion: 1.6.0
"@
$wPulex = Join-Path $releasesDir "$Publisher.Pulex.yaml"
[System.IO.File]::WriteAllText($wPulex, ($wContent -replace "`r`n", "`n") + "`n", (New-Object System.Text.UTF8Encoding($false)))

# 6. Cryptographic SHA256SUMS.txt
Write-Output "[STEP 6/6] Writing Cryptographic SHA256SUMS.txt..."
$sumsLines = @(
    "$($exeHash.ToLower())  $pExeName",
    "$($zipHash.ToLower())  $pZipName"
)
$sumsPath = Join-Path $releasesDir "SHA256SUMS.txt"
[System.IO.File]::WriteAllText($sumsPath, ($sumsLines -join "`n") + "`n", (New-Object System.Text.UTF8Encoding($false)))

$zipSizeMB = [math]::Round((Get-Item $zPulex).Length / 1MB, 2)
$exeSizeMB = [math]::Round((Get-Item $ePulex).Length / 1MB, 2)

Write-Output "======================================================================"
Write-Output "  PACKAGING COMPLETE"
Write-Output "  Root Dev Binary:  $projectRoot\pulex.exe ($exeSizeMB MB)"
Write-Output "  Portable EXE:     $ePulex ($exeSizeMB MB)"
Write-Output "  Portable ZIP:     $zPulex ($zipSizeMB MB)"
Write-Output "  Winget Manifest:  $wPulex"
Write-Output "  SHA256SUMS:       $sumsPath"
Write-Output "======================================================================"
Set-Location $projectRoot
