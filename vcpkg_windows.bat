mkdir build || goto :error
cd build || goto :error
git clone https://github.com/philjdf/vcpkg.git vcpkg.windows || goto :error
cd vcpkg.windows || goto :error
git checkout ArrowUpdateLinux || goto :error
call bootstrap-vcpkg.bat || goto :error

vcpkg.exe install boost-format:x64-windows-static || goto :error
vcpkg.exe install arrow:x64-windows-static || goto :error

cd ..
cd ..

exit /b


:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%