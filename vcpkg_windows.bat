mkdir build
cd build || goto :error
git clone https://github.com/GPSnoopy/vcpkg.git vcpkg.windows || goto :error
cd vcpkg.windows || goto :error
git checkout Arrow-0.17 || goto :error
call bootstrap-vcpkg.bat || goto :error

vcpkg.exe install arrow:x64-windows-static || goto :error

cd ..
cd ..

exit /b


:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%
