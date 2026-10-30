using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// A memory pool that forwards all allocations to another memory pool,
    /// while tracking its own statistics independently.
    /// </summary>
    /// <remarks>
    /// The statistics of a proxy memory pool only include allocations made through the proxy,
    /// so creating a separate proxy for each writer allows tracking the memory used by each writer,
    /// even when they share the same underlying memory pool.
    ///
    /// Unlike the global memory pools, a proxy memory pool owns a native resource.
    /// It must not be disposed until every object that allocates from it has been disposed,
    /// for example any writer, reader, properties or buffer it was passed to,
    /// and any Arrow arrays read using it.
    /// </remarks>
    public sealed class ProxyMemoryPool : MemoryPool, IDisposable
    {
        /// <summary>
        /// Create a new proxy memory pool that forwards allocations to the specified memory pool.
        /// </summary>
        /// <param name="wrappedPool">The memory pool to forward allocations to</param>
        public ProxyMemoryPool(MemoryPool wrappedPool)
            : this(CreateHandle(wrappedPool), wrappedPool)
        {
        }

        private ProxyMemoryPool(ProxyMemoryPoolHandle handle, MemoryPool wrappedPool)
            : base(handle.DangerousGetHandle())
        {
            _handle = handle;
            _wrappedPool = wrappedPool;
        }

        public void Dispose()
        {
            _handle.Dispose();
        }

        internal override IntPtr Handle
        {
            get
            {
                if (_handle.IsClosed)
                {
                    throw new NullReferenceException("null native handle");
                }

                return base.Handle;
            }
        }

        private static ProxyMemoryPoolHandle CreateHandle(MemoryPool wrappedPool)
        {
            if (wrappedPool == null) throw new ArgumentNullException(nameof(wrappedPool));

            var handle = new ProxyMemoryPoolHandle(ExceptionInfo.Return<IntPtr, IntPtr>(wrappedPool.Handle, MemoryPool_Proxy_Create));
            GC.KeepAlive(wrappedPool);
            return handle;
        }

        /// <summary>
        /// Using a SafeHandle rather than a ParquetHandle means the native pool is released by a critical finalizer,
        /// which runs after the normal finalizers of any objects using this pool that are garbage collected at the same time.
        /// </summary>
        private sealed class ProxyMemoryPoolHandle : SafeHandle
        {
            public ProxyMemoryPoolHandle(IntPtr handle)
                : base(IntPtr.Zero, ownsHandle: true)
            {
                SetHandle(handle);
            }

            public override bool IsInvalid => handle == IntPtr.Zero;

            protected override bool ReleaseHandle()
            {
                MemoryPool_Proxy_Free(handle);
                return true;
            }
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr MemoryPool_Proxy_Create(IntPtr wrappedPool, out IntPtr memoryPool);

        [DllImport(ParquetDll.Name)]
        private static extern void MemoryPool_Proxy_Free(IntPtr memoryPool);

        private readonly ProxyMemoryPoolHandle _handle;
        // Keep a reference to the wrapped pool, which may itself be a proxy that must outlive this one
        private readonly MemoryPool _wrappedPool;
    }
}
