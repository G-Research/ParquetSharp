using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public struct CacheOption
    {
        public CacheOption(long hole_size_limit, long range_size_limit, bool lazy, long prefetch_limit = 0)
        {
            this.hole_size_limit = hole_size_limit;
            this.range_size_limit = range_size_limit;
            this.lazy = lazy;
            this.prefetch_limit = prefetch_limit;
        }

        /// <summary>
        /// The maximum distance in bytes between two consecutive
        /// ranges; beyond this value, ranges are not combined
        /// </summary>
        public long hole_size_limit;

        /// <summary>
        /// The maximum size in bytes of a combined range; if
        /// combining two consecutive ranges would produce a range of a
        /// size greater than this, they are not combined
        /// </summary>
        public long range_size_limit;

        /// <summary>
        /// A lazy cache does not perform any I/O until requested.
        /// lazy = false: request all byte ranges when PreBuffer or WillNeed is called.
        /// lazy = True, prefetch_limit = 0: request merged byte ranges only after the reader
        /// needs them.
        /// lazy = True, prefetch_limit = k: prefetch up to k merged byte ranges ahead of the
        /// range that is currently being read.
        /// 
        /// Marshal as a 1-byte C/C++ bool so layout matches native code on all platforms.
        ///  </summary>
        [MarshalAs(UnmanagedType.I1)]
        public bool lazy;

        /// <summary>
        /// The maximum number of ranges to be prefetched. This is only used
        /// for lazy cache to asynchronously read some ranges after reading the target range.
        /// </summary>
        public long prefetch_limit;
    }
}
