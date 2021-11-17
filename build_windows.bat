set triplet=x64-windows-static
cmake -B build/%triplet% -S . -D VCPKG_TARGET_TRIPLET=%triplet% -D CMAKE_TOOLCHAIN_FILE=../vcpkg.%triplet%/scripts/buildsystems/vcpkg.cmake -G "Visual Studio 16 2019" -A "x64" || goto :error
msbuild build/%triplet%/ParquetSharp.sln -t:ParquetSharpNative:Rebuild -p:Configuration=Release || goto :error

exit /b


:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%