Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

# Find vcpkg or download it if required
if ($null -ne $Env:VCPKG_INSTALLATION_ROOT) {
  $vcpkgDir = $Env:VCPKG_INSTALLATION_ROOT
  Write-Output "Using vcpkg at $vcpkgDir from VCPKG_INSTALLATION_ROOT"
}
elseif ($null -ne $Env:VCPKG_ROOT) {
  $vcpkgDir = $Env:VCPKG_ROOT
  Write-Output "Using vcpkg at $vcpkgDir from VCPKG_ROOT"
}
else {
  $vcpkgDir = "$(Get-Location)/build/vcpkg"
  Write-Output "Using local vcpkg at $vcpkgDir"
  if (-not (Test-Path $vcpkgDir)) {
    git clone https://github.com/microsoft/vcpkg.git $vcpkgDir
    if (-not $?) { throw "git clone failed" }
    & $vcpkgDir/bootstrap-vcpkg.bat
    if (-not $?) { throw "bootstrap-vcpkg failed" }
  }
}

switch -Regex ($env:PROCESSOR_ARCHITECTURE) {
  "AMD64" { $arch = "x64" }
  "ARM64" { $arch = "arm64" }
  default { throw "Unsupported architecture: $env:PROCESSOR_ARCHITECTURE" }
}

$triplet = "$arch-windows-static"

$build_types = @("Debug", "Release")

# Ensure vcpkg uses the same MSVC version to build dependencies as we use to build the ParquetSharp library.
# By default, vcpkg uses the most recent version it can find, which might not be the same as what msbuild uses.
if ($null -eq $Env:VCToolsVersion) {
  $vsInstPath = & "${env:ProgramFiles(x86)}/Microsoft Visual Studio/Installer/vswhere.exe" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
  Import-Module "$vsInstPath/Common7/Tools/Microsoft.VisualStudio.DevShell.dll"
  Enter-VsDevShell -VsInstallPath $vsInstPath -SkipAutomaticLocation
}
$toolsetVersion = $Env:VCToolsVersion
Write-Output "Using platform toolset version = $toolsetVersion"
$generator = switch ($Env:VisualStudioVersion) {
  "17.0" { "Visual Studio 17 2022" }
  "18.0" { "Visual Studio 18 2026" }
  default { throw "Unsupported Visual Studio version: $Env:VisualStudioVersion" }
}

if ($Env:GITHUB_ACTIONS -eq "true") {
  $build_types = @("Release")
}

$customTripletsDir = "$(Get-Location)/build/custom-triplets"
New-Item -Path $customTripletsDir -ItemType "directory" -Force > $null
foreach ($subdir in @("", "community")) {
  $sourceTripletFile = "$vcpkgDir/triplets/$subdir/$triplet.cmake"
  if (Test-Path $sourceTripletFile) {
    $customTripletFile = "$customTripletsDir/$triplet.cmake"
    Copy-Item -Path $sourceTripletFile -Destination $customTripletFile
    if ($Env:GITHUB_ACTIONS -eq "true") {
      Add-Content -Path $customTripletFile -Value "set(VCPKG_BUILD_TYPE release)"
    }
    Add-Content -Path $customTripletFile -Value "set(VCPKG_PLATFORM_TOOLSET_VERSION $toolsetVersion)"
  }
}

cmake -B build/$triplet -S . -D VCPKG_TARGET_TRIPLET=$triplet -D VCPKG_OVERLAY_TRIPLETS=$customTripletsDir -D CMAKE_TOOLCHAIN_FILE=$vcpkgDir/scripts/buildsystems/vcpkg.cmake -G $generator -D CMAKE_GENERATOR_INSTANCE=$($Env:VSINSTALLDIR.TrimEnd("\")) -A $arch -T version=$toolsetVersion
if (-not $?) { throw "cmake failed" }

foreach ($build_type in $build_types) {
  cmake --build build/$triplet --target ParquetSharpNative --config $build_type
  if (-not $?) { throw "cmake build failed" }
}