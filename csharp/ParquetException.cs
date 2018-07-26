
using System;

namespace ParquetSharp
{
    /// <summary>
    /// Exception thrown by apache-parquet-cpp or the ParquetSharp wrapping logic.
    /// </summary>
    public sealed class ParquetException : Exception
    {
        public ParquetException(string type, string message)
            : base($"{type} (message: '{message}')")
        {
        }
    }
}