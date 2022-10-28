using System.IO;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    internal static class LongPath
    {
        /// <summary>
        /// Perform OS-dependent pre-processing of the specified path so that the win32 apis used by Arrow to open
        /// files will handle long paths.
        /// The host must also be configured to enable long paths, and the application manifest must specify that
        /// the application is long path aware.
        /// See https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation for more details.
        /// </summary>
        public static string EnsureLongPathSafe(string path)
        {
            if (!Path.IsPathRooted(path) || !RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return path;
            }
            if (path.StartsWith("//?/") || path.StartsWith(@"\\?\"))
            {
                return path;
            }
            if (path.StartsWith("//") || path.StartsWith(@"\\"))
            {
                return @"\\?\UNC\" + path.Substring(2);
            }
            return @"\\?\" + path;
        }
    }
}
