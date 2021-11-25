for /f "tokens=1,2" %%a in (vcpkg_version.txt) do (
  set vcpkg_url=%%a
  set vcpkg_ref=%%b
) || goto :error

set triplet=x64-windows-static

mkdir build
cd build || goto :error
rem Clone without checking out a branch, as vcpkg_ref could be a commit SHA
git clone %vcpkg_url% vcpkg.%triplet% || goto :error
cd vcpkg.%triplet% || goto :error
git checkout %vcpkg_ref% || goto :error
if "%GITHUB_ACTIONS%"=="true" echo set(VCPKG_BUILD_TYPE release) >> triplets\%triplet%.cmake || goto :error
call .\bootstrap-vcpkg.bat || goto :error

.\vcpkg.exe install arrow:%triplet% || goto :error

cd ..
cd ..

exit /b


:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%
