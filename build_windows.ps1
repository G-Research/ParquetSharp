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
if ($Env:GITHUB_ACTIONS -eq "true") {
  $build_types = @("Release")
}

# Use a single Visual Studio instance and MSVC toolset for vcpkg dependencies, cmake and msbuild.
# vcpkg defaults to the newest toolset it can find. If cmake/msbuild use an older one, arrow.lib and
# parquet.lib reference STL helpers that the older STL does not provide and the link fails (LNK2019).
$vswhere = "${env:ProgramFiles(x86)}/Microsoft Visual Studio/Installer/vswhere.exe"
$vsInstPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
if ([string]::IsNullOrEmpty($vsInstPath)) { throw "No Visual Studio instance with the MSVC toolset was found" }
$vsMajor = (& $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationVersion).Split(".")[0]
$generator = switch ($vsMajor) {
  "17" { "Visual Studio 17 2022" }
  "18" { "Visual Studio 18 2026" }
  default { throw "Unsupported Visual Studio major version: $vsMajor" }
}
Import-Module "$vsInstPath/Common7/Tools/Microsoft.VisualStudio.DevShell.dll"
Enter-VsDevShell -VsInstallPath $vsInstPath -SkipAutomaticLocation
# Take the toolset and cmake from the selected instance rather than from PATH, which may still point at another VS.
$toolsetVersion = (Get-Content "$vsInstPath/VC/Auxiliary/Build/Microsoft.VCToolsVersion.default.txt" -First 1).Trim()
if (-not $toolsetVersion.StartsWith("14.")) { throw "Couldn't get toolset version from '$vsInstPath'" }
$cmake = "$vsInstPath/Common7/IDE/CommonExtensions/Microsoft/CMake/CMake/bin/cmake.exe"
if (-not (Test-Path $cmake)) { throw "cmake.exe not found at $cmake (install the C++ CMake tools for Windows component)" }
Write-Output "Using Visual Studio at $vsInstPath"
Write-Output "Using platform toolset version = $toolsetVersion"
Write-Output "Using cmake at $cmake"
Write-Output "Using generator $generator"

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

& $cmake -B build/$triplet -S . -D VCPKG_TARGET_TRIPLET=$triplet -D VCPKG_OVERLAY_TRIPLETS=$customTripletsDir -D CMAKE_TOOLCHAIN_FILE=$vcpkgDir/scripts/buildsystems/vcpkg.cmake -G $generator -D CMAKE_GENERATOR_INSTANCE=$vsInstPath -A $arch -T version=$toolsetVersion
if (-not $?) { throw "cmake failed" }

foreach ($build_type in $build_types) {
  & $cmake --build build/$triplet --target ParquetSharpNative --config $build_type --clean-first
  if (-not $?) { throw "cmake build failed" }
}
