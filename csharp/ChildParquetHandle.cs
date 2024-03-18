using System;

namespace ParquetSharp
{
    /// <summary>
    /// Holds a pointer to a native object that we don't have direct ownership of,
    /// but is a child of another object we own.
    /// Required when a C++ class method returns a raw pointer.
    /// </summary>
    internal sealed class ChildParquetHandle : INativeHandle
    {
        public ChildParquetHandle(IntPtr handle, ParquetHandle parentHandle)
        {
            _handle = handle;
            _parentHandle = parentHandle;
        }

        public IntPtr IntPtr
        {
            get
            {
                if (_parentHandle.Disposed)
                {
                    throw new NullReferenceException(
                        "Attempted to access an object whose owning parent has been disposed");
                }
                return _handle;
            }
        }

        public void Dispose()
        {
        }

        private readonly IntPtr _handle;
        private readonly ParquetHandle _parentHandle;
    }
}
