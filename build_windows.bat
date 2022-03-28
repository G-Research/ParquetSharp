set triplet=x64-windows-static
cmake -B build/%triplet% -S . -D VCPKG_TARGET_TRIPLET=%triplet% -D CMAKE_TOOLCHAIN_FILE=../vcpkg.%triplet%/scripts/buildsystems/vcpkg.cmake -D VCPKG_INSTALLED_DIR=../vcpkg.%triplet%/installed -G "Visual Studio 17 2022" -A "x64" || goto :error
msbuild build/%triplet%/ParquetSharp.sln -t:ParquetSharpNative:Rebuild -p:Configuration=Release || goto :error

exit /b


:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%