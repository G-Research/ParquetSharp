using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;

namespace ParquetSharp
{
    /// <summary>
    /// Cache duplicated ByteArray / FixedByteArray values when reading and converting them to their logical form.
    /// This is particularly useful when reading a lot of duplicate strings.
    /// </summary>
    public sealed class ByteArrayReaderCache<TPhysical, TLogical>
        where TPhysical : unmanaged
    {
        public ByteArrayReaderCache(ColumnChunkMetaData columnChunkMetaData)
        {
            // If dictionary encoding is used, it's worth caching repeated values for byte arrays.
            _map = columnChunkMetaData.Encodings.Any(e => e == Encoding.PlainDictionary || e == Encoding.RleDictionary) &&
                   (typeof(TPhysical) == typeof(ByteArray) || typeof(TPhysical) == typeof(FixedLenByteArray))
                ? new Dictionary<TPhysical, TLogical>()
                : null;

            _scratch = new byte[64];
        }

        public bool IsUsable => _map != null;

        public void Clear()
        {
            if (_map == null) throw new InvalidOperationException("cache is not in a usable state");
            _map.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TLogical Add(TPhysical physical, TLogical logical)
        {
            if (_map == null) throw new InvalidOperationException("cache is not in a usable state");
            _map.Add(physical, logical);
            return logical;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if (NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER)
        public bool TryGetValue(TPhysical physical, [MaybeNullWhen(false)] out TLogical logical)
#else
        public bool TryGetValue(TPhysical physical, out TLogical logical)
#endif
        {
            if (_map == null) throw new InvalidOperationException("cache is not in a usable state");
            return _map.TryGetValue(physical, out logical);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] GetScratchBuffer(int minLength)
        {
            return _scratch.Length >= minLength ? _scratch : _scratch = new byte[Math.Max(_scratch.Length * 2, minLength)];
        }

        private readonly Dictionary<TPhysical, TLogical>? _map;
        private byte[] _scratch;
    }
}
