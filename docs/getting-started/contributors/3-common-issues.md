### Known Issues
An issue that may occur when building ParquetSharp locally using `build_windows.ps1` is Visual Studio not being detected by CMake:
```pwsh
CMake Error at CMakeLists.txt:2 (project):   Generator

  Visual Studio 17 2022

could not find any instance of Visual Studio.
```
This is a known issue: [(1)](https://stackoverflow.com/questions/60068168/cmake-problem-could-not-find-any-instance-of-visual-studio) [(2)](https://stackoverflow.com/questions/59953960/cmake-and-vs-2017-could-not-find-any-instance-of-visual-studio). It can be solved by ensuring that all required Visual Studio Build Tools are properly installed and that the relevant version of Visual Studio is available, and finally rebooting the machine. Another potential solution is to reinstall Visual Studio with the required build tools.

When building, you may come across the following problem with `Microsoft.Cpp.Default.props`: 
```pwsh
error MSB4019: The imported project "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\MSBuild\Microsoft\V
C\v170\Microsoft.Cpp.Default.props" was not found. Confirm that the expression in the Import declaration "C:\Program Fi
les (x86)\Microsoft Visual Studio\2022\BuildTools\MSBuild\Microsoft\VC\v170\\Microsoft.Cpp.Default.props" is correct, a
nd that the file exists on disk.
```
To resolve this, make sure that the "Desktop development with C++" option is selected when installing Visual Studio Build Tools. If installation is successful, the required directory and files should be present. 

Another common issue is the following:
```pwsh
CMake Error at CMakeLists.txt:2 (project):
  The CMAKE_C_COMPILER:

    C:/Program Files/Microsoft Visual Studio/2022/Community/VC/Tools/MSVC/14.37.32822/bin/Hostx64/x64/cl.exe

  is not a full path to an existing compiler tool.

CMake Error at CMakeLists.txt:2 (project):
  The CMAKE_CXX_COMPILER:

    C:/Program Files/Microsoft Visual Studio/2022/Community/VC/Tools/MSVC/14.37.32822/bin/Hostx64/x64/cl.exe

  is not a full path to an existing compiler tool.
```
This is also related to installed Visual Studio modules. Make sure to install "C++/CLI support for build tools" from the list of optional components for Desktop development with C++ for the relevant version of Visual Studio.

For any other build issues, please [open a new discussion](https://github.com/G-Research/ParquetSharp/discussions).
