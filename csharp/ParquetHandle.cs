using System;

namespace ParquetSharp
{
    /// <summary>
    /// Associate a native handle with its corresponding resource release method.
    /// </summary>
    internal sealed class ParquetHandle : IDisposable
    {
        public ParquetHandle(IntPtr handle, Action<IntPtr> free)
        {
            _handle = handle;
            _free = free;
        }

        public void Dispose()
        {
            if (_handle != IntPtr.Zero)
            {
                _free(_handle);
                _handle = IntPtr.Zero;
            }

            GC.SuppressFinalize(this);
        }

        ~ParquetHandle()
        {
            if (_handle != IntPtr.Zero)
            {
                _free(_handle);
                _handle = IntPtr.Zero;
            }
        }

        public IntPtr IntPtr
        {
            get
            {
                // Check the handle is not null. 
                // This situation Usually happens when the parent class has already been disposed.
                if (_handle == IntPtr.Zero)
                {
                    throw new NullReferenceException("null native handle");
                }

                return _handle;
            }
        }

        private IntPtr _handle;
        private readonly Action<IntPtr> _free;
    }
}