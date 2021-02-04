cd build || goto :error
mkdir Windows || goto :error
cd Windows || goto :error
cmake -D VCPKG_TARGET_TRIPLET=x64-windows-static -D CMAKE_TOOLCHAIN_FILE=../vcpkg.Windows/scripts/buildsystems/vcpkg.cmake -G "Visual Studio 16 2019" -A "x64" ../.. || goto :error
msbuild ParquetSharp.sln -t:ParquetSharpNative:Rebuild -p:Configuration=Release || goto :error
cd ..
cd ..

exit /b


:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%