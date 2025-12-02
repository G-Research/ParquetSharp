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

$options = @()
if ($Env:GITHUB_ACTIONS -eq "true") {
  $build_types = @("Release")
  $customTripletsDir = "$(Get-Location)/build/custom-triplets"
  New-Item -Path $customTripletsDir -ItemType "directory" -Force > $null
  foreach ($subdir in @("", "community")) {
    $sourceTripletFile = "$vcpkgDir/triplets/$subdir/$triplet.cmake"
    if (Test-Path $sourceTripletFile) {
      $customTripletFile = "$customTripletsDir/$triplet.cmake"
      Copy-Item -Path $sourceTripletFile -Destination $customTripletFile
      Add-Content -Path $customTripletFile -Value "set(VCPKG_BUILD_TYPE release)"

      # Ensure vcpkg uses the same MSVC version to build dependencies as we use to build the ParquetSharp library.
      # By default, vcpkg uses the most recent version it can find, which might not be the same as what msbuild uses.
      $vsInstPath = & "${env:ProgramFiles(x86)}/Microsoft Visual Studio/Installer/vswhere.exe" -latest -property installationPath
      Import-Module "$vsInstPath/Common7/Tools/Microsoft.VisualStudio.DevShell.dll"
      Enter-VsDevShell -VsInstallPath $vsInstPath -SkipAutomaticLocation
      $clPath = Get-Command cl.exe | Select -ExpandProperty "Source"
      $toolsetVersion = $clPath.Split("\")[8]
      if (-not $toolsetVersion.StartsWith("14.")) { throw "Couldn't get toolset version from path '$clPath'" }
      Write-Output "Using platform toolset version = $toolsetVersion"
      Add-Content -Path $customTripletFile -Value "set(VCPKG_PLATFORM_TOOLSET_VERSION $toolsetVersion)"
    }
  }
  $options += "-D"
  $options += "VCPKG_OVERLAY_TRIPLETS=$customTripletsDir"
}


$options += " -DCMAKE_VERBOSE_MAKEFILE=ON"

cmake -B build/$triplet -S . -D VCPKG_TARGET_TRIPLET=$triplet -D CMAKE_TOOLCHAIN_FILE=$vcpkgDir/scripts/buildsystems/vcpkg.cmake -G "Visual Studio 17 2022" -A $arch $options
if (-not $?) { throw "cmake failed" }

foreach ($build_type in $build_types) {
  msbuild build/$triplet/ParquetSharp.sln -t:ParquetSharpNative:Rebuild -p:Configuration=$build_type
  if (-not $?) { throw "msbuild failed" }
}