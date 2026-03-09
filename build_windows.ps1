Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

# Pin vcpkg to the same VS installation used by the Developer Shell.
# VSINSTALLDIR is set when running from a VS Developer PowerShell.
if ($null -ne $Env:VSINSTALLDIR) {
  $Env:VCPKG_VISUAL_STUDIO_PATH = $Env:VSINSTALLDIR.TrimEnd('\')
  Write-Output "Using VCPKG_VISUAL_STUDIO_PATH at $Env:VCPKG_VISUAL_STUDIO_PATH"
}
else {
  $vsInstPath = & "${env:ProgramFiles(x86)}/Microsoft Visual Studio/Installer/vswhere.exe" -latest -property installationPath
  $Env:VCPKG_VISUAL_STUDIO_PATH = $vsInstPath.TrimEnd('\')
  Write-Output "Using VCPKG_VISUAL_STUDIO_PATH at $Env:VCPKG_VISUAL_STUDIO_PATH"
}

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
$options = @()

# Buildd release configuration only
if ($Env:GITHUB_ACTIONS -eq "true") {
  $build_types = @("Release")
}

# Build dependencies in release configuration only
$customTripletsDir = "$(Get-Location)/build/custom-triplets"
New-Item -Path $customTripletsDir -ItemType "directory" -Force > $null
foreach ($subdir in @("", "community")) {
	$sourceTripletFile = "$vcpkgDir/triplets/$subdir/$triplet.cmake"
	if (Test-Path $sourceTripletFile) {
	  $customTripletFile = "$customTripletsDir/$triplet.cmake"
	  Copy-Item -Path $sourceTripletFile -Destination $customTripletFile
	  Add-Content -Path $customTripletFile -Value "set(VCPKG_BUILD_TYPE release)"
	}
}
$options += "-DVCPKG_OVERLAY_TRIPLETS=$customTripletsDir"
$options += "-DCMAKE_VERBOSE_MAKEFILE=ON"

cmake -B build/$triplet -S . -D VCPKG_TARGET_TRIPLET=$triplet -D CMAKE_TOOLCHAIN_FILE=$vcpkgDir/scripts/buildsystems/vcpkg.cmake -G "Visual Studio 17 2022" -A $arch @options
if (-not $?) { throw "cmake failed" }

foreach ($build_type in $build_types) {
  cmake --build build/$triplet --target ParquetSharpNative --config $build_type
  if (-not $?) { throw "cmake build failed" }
}