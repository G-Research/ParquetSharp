rem Find vcpkg or download it if required
if not defined VCPKG_INSTALLATION_ROOT (
  if defined VCPKG_ROOT (
    set VCPKG_INSTALLATION_ROOT=%VCPKG_ROOT%
  ) else (
    set VCPKG_INSTALLATION_ROOT=%cd%\build\vcpkg
    if not exist %cd%\build\vcpkg (
      git clone https://github.com/microsoft/vcpkg.git %cd%\build\vcpkg || goto :error
      call %cd%\build\vcpkg\bootstrap-vcpkg.bat || goto :error
    )
  )
)

set triplet=x64-windows-static

set options=-D VCPKG_TARGET_TRIPLET=%triplet% -D CMAKE_TOOLCHAIN_FILE=%VCPKG_INSTALLATION_ROOT%/scripts/buildsystems/vcpkg.cmake
if "%GITHUB_ACTIONS%"=="true" (
  mkdir custom-triplets || goto :error
  copy "%VCPKG_INSTALLATION_ROOT%\triplets\%triplet%.cmake" "custom-triplets\%triplet%.cmake" || goto :error
  echo set(VCPKG_BUILD_TYPE release^) >> custom-triplets\%triplet%.cmake || goto :error
  set options=%options% -D VCPKG_OVERLAY_TRIPLETS=%cd%/custom-triplets
)

cmake -B build/%triplet% -S . %options% -G "Visual Studio 17 2022" -A "x64" || goto :error
msbuild build/%triplet%/ParquetSharp.sln -t:ParquetSharpNative:Rebuild -p:Configuration=Release || goto :error

exit /b

:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%
