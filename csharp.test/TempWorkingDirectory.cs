using System;
using System.IO;

namespace ParquetSharp.Test
{
    internal sealed class TempWorkingDirectory : IDisposable
    {
        public TempWorkingDirectory()
        {
            _originalWorkingDirectory = Directory.GetCurrentDirectory();
            _directoryPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(_directoryPath);
            Directory.SetCurrentDirectory(_directoryPath);
        }

        public void Dispose()
        {
            Directory.SetCurrentDirectory(_originalWorkingDirectory);
            Directory.Delete(_directoryPath, recursive: true);
        }

        public string DirectoryPath => _directoryPath;

        private readonly string _directoryPath;
        private readonly string _originalWorkingDirectory;
    }
}
