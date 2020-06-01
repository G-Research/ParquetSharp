for /f "tokens=1,2" %%a in (vcpkg_version.txt) do (
  set vcpkg_url=%%a
  set vcpkg_ref=%%b
) || goto :error

mkdir build
cd build || goto :error
git clone %vcpkg_url% -b %vcpkg_ref% vcpkg.windows || goto :error
cd vcpkg.windows || goto :error
if "%GITHUB_ACTIONS%"=="true" echo set(VCPKG_BUILD_TYPE release) >> triplets\x64-windows-static.cmake || goto :error
call bootstrap-vcpkg.bat || goto :error

vcpkg.exe install arrow:x64-windows-static || goto :error

cd ..
cd ..

exit /b


:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%
