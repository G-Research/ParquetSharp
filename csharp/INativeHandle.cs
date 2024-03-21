using System;

namespace ParquetSharp
{
    internal interface INativeHandle : IDisposable
    {
        IntPtr IntPtr { get; }

        bool Disposed { get; }
    }
}
