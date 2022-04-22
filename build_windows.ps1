Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

# Find vcpkg or download it if required
if ($Env:VCPKG_INSTALLATION_ROOT -ne $null) {
  $vcpkgDir = $Env:VCPKG_INSTALLATION_ROOT
  echo "Using vcpkg at $vcpkgDir from VCPKG_INSTALLATION_ROOT"
} elseif ($Env:VCPKG_ROOT -ne $null) {
  $vcpkgDir = $Env:VCPKG_ROOT
  echo "Using vcpkg at $vcpkgDir from VCPKG_ROOT"
} else {
  $vcpkgDir = "$(pwd)/build/vcpkg"
  echo "Using local vcpkg at $vcpkgDir"
  if (-not (Test-Path $vcpkgDir)) {
    git clone https://github.com/microsoft/vcpkg.git $vcpkgDir
    if (-not $?) { throw "git clone failed" }
    & $vcpkgDir/bootstrap-vcpkg.bat
    if (-not $?) { throw "bootstrap-vcpkg failed" }
  }
}

$triplet = "x64-windows-static"

$options = @()
if ($Env:GITHUB_ACTIONS -eq "true") {
  $customTripletsDir = "$(pwd)/build/custom-triplets"
  New-Item -Path $customTripletsDir -ItemType "directory" -Force > $null
  $sourceTripletFile = "$vcpkgDir/triplets/$triplet.cmake"
  $customTripletFile = "$customTripletsDir/$triplet.cmake"
  Copy-Item -Path $sourceTripletFile -Destination $customTripletFile
  Add-Content -Path $customTripletFile -Value "set(VCPKG_BUILD_TYPE release)"
  $options += "-D"
  $options += "VCPKG_OVERLAY_TRIPLETS=$customTripletsDir"
}

cmake -B build/$triplet -S . -D VCPKG_TARGET_TRIPLET=$triplet -D CMAKE_TOOLCHAIN_FILE=$vcpkgDir/scripts/buildsystems/vcpkg.cmake -G "Visual Studio 17 2022" -A "x64" $options
if (-not $?) { throw "cmake failed" }
msbuild build/$triplet/ParquetSharp.sln -t:ParquetSharpNative:Rebuild -p:Configuration=Release
if (-not $?) { throw "msbuild failed" }
