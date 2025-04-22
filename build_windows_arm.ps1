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

$triplet = "arm64-windows"

$build_types = @("Debug", "Release")

if ($null -eq $env:ARROW_HOME) {
    Write-Host "Error: ARROW_HOME environment variable not set." -ForegroundColor Red
    Write-Host "Please set it to your Arrow installation directory, for example:" -ForegroundColor Yellow
    Write-Host "    $env:ARROW_HOME = 'C:\path\to\arrow\install\$triplet'" -ForegroundColor Yellow
    exit 1
}

$arrow_install_dir = $env:ARROW_HOME

if (-not (Test-Path "$arrow_install_dir/release") -or -not (Test-Path "$arrow_install_dir/debug")) {
    Write-Host "Error: ARROW_HOME directory doesn't contain expected 'debug' and 'release' subdirectories." -ForegroundColor Red
    Write-Host "Please ensure your Arrow installation has both debug and release builds." -ForegroundColor Yellow
    exit 1
}

Write-Host "Using Arrow installation from: $arrow_install_dir" -ForegroundColor Green

$options = @()
if ($Env:GITHUB_ACTIONS -eq "true") {
  $build_types = @("Release")
  $customTripletsDir = "$(Get-Location)/build/custom-triplets"
  New-Item -Path $customTripletsDir -ItemType "directory" -Force > $null
  $sourceTripletFile = "$vcpkgDir/triplets/$triplet.cmake"
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

  $options += "-D"
  $options += "VCPKG_OVERLAY_TRIPLETS=$customTripletsDir"
}

cmake -B build/$triplet -S . `
  -G "Ninja Multi-Config" `
  -DCMAKE_C_COMPILER=clang-cl `
  -DCMAKE_CXX_COMPILER=clang-cl `
  -DCMAKE_TOOLCHAIN_FILE=C:/src/vcpkg/scripts/buildsystems/vcpkg.cmake `
  -DCMAKE_MAKE_PROGRAM=ninja `
  -DVCPKG_TARGET_TRIPLET="$triplet" `
  -DARROW_ROOT_DEBUG="$arrow_install_dir/debug" `
  -DARROW_ROOT_RELEASE="$arrow_install_dir/release" `
  $options

if (-not $?) { throw "cmake failed" }

foreach ($build_type in $build_types) {
  cmake --build build/$triplet --config $build_type --target ParquetSharpNative
  if (-not $?) { throw "ninja build failed" }
}