set /p VCPKG_VERSION=<vcpkg_version.txt || goto :error
mkdir build
cd build || goto :error
git clone https://github.com/microsoft/vcpkg.git vcpkg.windows || goto :error
cd vcpkg.windows || goto :error
git checkout %VCPKG_VERSION% || goto :error
call bootstrap-vcpkg.bat || goto :error

vcpkg.exe install arrow:x64-windows-static || goto :error

cd ..
cd ..

exit /b


:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%
