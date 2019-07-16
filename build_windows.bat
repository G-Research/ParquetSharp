cd build || goto :error
mkdir windows || goto :error
cd windows || goto :error
cmake -D CMAKE_PREFIX_PATH=../build/vcpkg.windows/installed/x64-windows-static/ -G "Visual Studio 16 2019" -A "x64" ../.. || goto :error
msbuild ParquetSharp.sln /t:restore || goto :error
msbuild ParquetSharp.sln /t:Rebuild /p:Configuration=Release || goto :error
cd ..
cd ..

exit /b


:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%