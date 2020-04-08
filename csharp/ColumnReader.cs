using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Reader of physical Parquet values from a single column.
    /// </summary>
    public abstract class ColumnReader : IDisposable
    {
        internal static ColumnReader Create(IntPtr handle, ColumnChunkMetaData columnChunkMetaData)
        {
            var parquetHandle = new ParquetHandle(handle, ColumnReader_Free);

            try
            {
                var type = ExceptionInfo.Return<PhysicalType>(handle, ColumnReader_Type);

                switch (type)
                {
                    case PhysicalType.Boolean:
                        return new ColumnReader<bool>(parquetHandle, columnChunkMetaData);
                    case PhysicalType.Int32:
                        return new ColumnReader<int>(parquetHandle, columnChunkMetaData);
                    case PhysicalType.Int64:
                        return new ColumnReader<long>(parquetHandle, columnChunkMetaData);
                    case PhysicalType.Int96:
                        return new ColumnReader<Int96>(parquetHandle, columnChunkMetaData);
                    case PhysicalType.Float:
                        return new ColumnReader<float>(parquetHandle, columnChunkMetaData);
                    case PhysicalType.Double:
                        return new ColumnReader<double>(parquetHandle, columnChunkMetaData);
                    case PhysicalType.ByteArray:
                        return new ColumnReader<ByteArray>(parquetHandle, columnChunkMetaData);
                    case PhysicalType.FixedLenByteArray:
                        return new ColumnReader<FixedLenByteArray>(parquetHandle, columnChunkMetaData);
                    default:
                        throw new NotSupportedException($"Physical type {type} is not supported");
                }
            }

            catch
            {
                parquetHandle.Dispose();
                throw;
            }
        }

        internal ColumnReader(ParquetHandle handle, ColumnChunkMetaData columnChunkMetaData)
        {
            Handle = handle;
            ColumnChunkMetaData = columnChunkMetaData;
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public ColumnDescriptor ColumnDescriptor => new ColumnDescriptor(ExceptionInfo.Return<IntPtr>(Handle, ColumnReader_Descr));
        public bool HasNext => ExceptionInfo.Return<bool>(Handle, ColumnReader_HasNext);
        public PhysicalType Type => ExceptionInfo.Return<PhysicalType>(Handle, ColumnReader_Type);

        public abstract Type ElementType { get; }
        public abstract TReturn Apply<TReturn>(IColumnReaderVisitor<TReturn> visitor);
        public abstract long Skip(long numRowsToSkip);

        public LogicalColumnReader LogicalReader(int bufferLength = 4 * 1024)
        {
            return LogicalColumnReader.Create(this, bufferLength);
        }

        public LogicalColumnReader<TElement> LogicalReader<TElement>(int bufferLength = 4 * 1024)
        {
            return LogicalColumnReader.Create<TElement>(this, bufferLength);
        }

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnReader_Free(IntPtr columnReader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnReader_Descr(IntPtr columnReader, out IntPtr columnDescriptor);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnReader_HasNext(IntPtr columnReader, [MarshalAs(UnmanagedType.I1)] out bool hasNext);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnReader_Type(IntPtr columnReader, out PhysicalType type);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Bool(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, bool* values, 
            out long valuesRead, out long levelsRead);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Int32(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, int* values, 
            out long valuesRead, out long levelsRead);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Int64(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, long* values, 
            out long valuesRead, out long levelsRead);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Int96(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, Int96* values, 
            out long valuesRead, out long levelsRead);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Float(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, float* values, 
            out long valuesRead, out long levelsRead);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Double(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, double* values, 
            out long valuesRead, out long levelsRead);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_ByteArray(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, ByteArray* values, 
            out long valuesRead, out long levelsRead);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_FixedLenByteArray(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, FixedLenByteArray* values, out long valuesRead, out long levelsRead);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatchSpaced_Bool(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, bool* values, byte* validBits, long validBitsOffset, 
            out long levelsRead, out long valuesRead, out long nullCount, out long returnValue);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatchSpaced_Int32(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, int* values, byte* validBits, long validBitsOffset, 
            out long levelsRead, out long valuesRead, out long nullCount, out long returnValue);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatchSpaced_Int64(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, long* values, byte* validBits, long validBitsOffset, 
            out long levelsRead, out long valuesRead, out long nullCount, out long returnValue);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatchSpaced_Int96(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, Int96* values, byte* validBits, long validBitsOffset, 
            out long levelsRead, out long valuesRead, out long nullCount, out long returnValue);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatchSpaced_Float(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, float* values, byte* validBits, long validBitsOffset,
            out long levelsRead, out long valuesRead, out long nullCount, out long returnValue);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatchSpaced_Double(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, double* values, byte* validBits, long validBitsOffset,
            out long levelsRead, out long valuesRead, out long nullCount, out long returnValue);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatchSpaced_ByteArray(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, ByteArray* values, byte* validBits, long validBitsOffset,
            out long levelsRead, out long valuesRead, out long nullCount, out long returnValue);

        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatchSpaced_FixedLenByteArray(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, FixedLenByteArray* values, byte* validBits, long validBitsOffset, 
            out long levelsRead, out long valuesRead, out long nullCount, out long returnValue);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Bool(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Int32(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Int64(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Int96(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Float(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Double(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_ByteArray(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_FixedLenByteArray(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        internal readonly ParquetHandle Handle;
        internal readonly ColumnChunkMetaData ColumnChunkMetaData;
    }

    /// <inheritdoc />
    public sealed class ColumnReader<TValue> : ColumnReader where TValue : unmanaged
    {
        internal ColumnReader(ParquetHandle handle, ColumnChunkMetaData columnChunkMetaData) 
            : base(handle, columnChunkMetaData)
        {
        }

        public override Type ElementType => typeof(TValue);

        public override TReturn Apply<TReturn>(IColumnReaderVisitor<TReturn> visitor)
        {
            return visitor.OnColumnReader(this);
        }

        public long ReadBatch(long batchSize, Span<TValue> values, out long valuesRead)
        {
            return ReadBatch(batchSize, null, null, values, out valuesRead);
        }

        public unsafe long ReadBatch(long batchSize, Span<short> defLevels, Span<short> repLevels, Span<TValue> values, out long valuesRead)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (values.Length < batchSize) throw new ArgumentOutOfRangeException(nameof(values), "batchSize is larger than length of values");
            if (defLevels != null && defLevels.Length < batchSize) throw new ArgumentOutOfRangeException(nameof(defLevels), "batchSize is larger than length of defLevels");
            if (repLevels != null && repLevels.Length < batchSize) throw new ArgumentOutOfRangeException(nameof(repLevels), "batchSize is larger than length of repLevels");

            var type = typeof(TValue);

            fixed (short* pDefLevels = defLevels)
            fixed (short* pRepLevels = repLevels)
            fixed (TValue* pValues = values)
            {
                if (type == typeof(bool))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatch_Bool(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (bool*) pValues, out valuesRead, out var levelsRead));
                    GC.KeepAlive(Handle);
                    return levelsRead;
                }

                if (type == typeof(int))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatch_Int32(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (int*) pValues, out valuesRead, out var levelsRead));
                    GC.KeepAlive(Handle);
                    return levelsRead;
                }

                if (type == typeof(long))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatch_Int64(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (long*) pValues, out valuesRead, out var levelsRead));
                    GC.KeepAlive(Handle);
                    return levelsRead;
                }

                if (type == typeof(Int96))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatch_Int96(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (Int96*) pValues, out valuesRead, out var levelsRead));
                    GC.KeepAlive(Handle);
                    return levelsRead;
                }

                if (type == typeof(float))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatch_Float(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (float*) pValues, out valuesRead, out var levelsRead));
                    GC.KeepAlive(Handle);
                    return levelsRead;
                }

                if (type == typeof(double))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatch_Double(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (double*) pValues, out valuesRead, out var levelsRead));
                    GC.KeepAlive(Handle);
                    return levelsRead;
                }

                if (type == typeof(ByteArray))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatch_ByteArray(Handle.IntPtr,
                        batchSize, pDefLevels, pRepLevels, (ByteArray*) pValues, out valuesRead, out var levelsRead));
                    GC.KeepAlive(Handle);
                    return levelsRead;
                }

                if (type == typeof(FixedLenByteArray))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatch_FixedLenByteArray(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (FixedLenByteArray*) pValues, out valuesRead, out var levelsRead));
                    GC.KeepAlive(Handle);
                    return levelsRead;
                }

                throw new NotSupportedException($"type {type} is not supported");
            }
        }

        public unsafe long ReadBatchSpaced(
            long batchSize, Span<short> defLevels, Span<short> repLevels, Span<TValue> values, Span<byte> validBits, long validBitsOffset, 
            out long levelsRead, out long valuesRead, out long nullCount)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (defLevels == null) throw new ArgumentNullException(nameof(defLevels));
            if (repLevels == null) throw new ArgumentNullException(nameof(repLevels));
            if (validBits == null) throw new ArgumentNullException(nameof(validBits));
            if (values.Length < batchSize) throw new ArgumentOutOfRangeException(nameof(values), "batchSize is larger than length of values");
            if (defLevels.Length < batchSize) throw new ArgumentOutOfRangeException(nameof(defLevels), "batchSize is larger than length of defLevels");
            if (repLevels.Length < batchSize) throw new ArgumentOutOfRangeException(nameof(repLevels), "batchSize is larger than length of repLevels");
            if (validBits.Length < (validBitsOffset + batchSize) / 8 + 1) throw new ArgumentOutOfRangeException(nameof(validBits), "batchSize is larger than the bit length of validBits");

            var type = typeof(TValue);

            fixed (short* pDefLevels = defLevels)
            fixed (short* pRepLevels = repLevels)
            fixed (byte* pValidBits = validBits)
            fixed (TValue* pValues = values)
            {
                if (type == typeof(bool))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatchSpaced_Bool(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (bool*) pValues, pValidBits, validBitsOffset, 
                        out levelsRead, out valuesRead, out nullCount, out var returnValue));
                    GC.KeepAlive(Handle);
                    return returnValue;
                }

                if (type == typeof(int))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatchSpaced_Int32(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (int*) pValues, pValidBits, validBitsOffset,
                        out levelsRead, out valuesRead, out nullCount, out var returnValue));
                    GC.KeepAlive(Handle);
                    return returnValue;
                }

                if (type == typeof(long))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatchSpaced_Int64(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (long*) pValues, pValidBits, validBitsOffset, 
                        out levelsRead, out valuesRead, out nullCount, out var returnValue));
                    GC.KeepAlive(Handle);
                    return returnValue;
                }

                if (type == typeof(Int96))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatchSpaced_Int96(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (Int96*) pValues, pValidBits, validBitsOffset, 
                        out levelsRead, out valuesRead, out nullCount, out var returnValue));
                    GC.KeepAlive(Handle);
                    return returnValue;
                }

                if (type == typeof(float))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatchSpaced_Float(Handle.IntPtr,
                        batchSize, pDefLevels, pRepLevels, (float*) pValues, pValidBits, validBitsOffset, 
                        out levelsRead, out valuesRead, out nullCount, out var returnValue));
                    GC.KeepAlive(Handle);
                    return returnValue;
                }

                if (type == typeof(double))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatchSpaced_Double(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (double*) pValues, pValidBits, validBitsOffset, 
                        out levelsRead, out valuesRead, out nullCount, out var returnValue));
                    GC.KeepAlive(Handle);
                    return returnValue;
                }

                if (type == typeof(ByteArray))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatchSpaced_ByteArray(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (ByteArray*) pValues, pValidBits, validBitsOffset, 
                        out levelsRead, out valuesRead, out nullCount, out var returnValue));
                    GC.KeepAlive(Handle);
                    return returnValue;
                }

                if (type == typeof(FixedLenByteArray))
                {
                    ExceptionInfo.Check(TypedColumnReader_ReadBatchSpaced_FixedLenByteArray(Handle.IntPtr, 
                        batchSize, pDefLevels, pRepLevels, (FixedLenByteArray*) pValues, pValidBits, validBitsOffset, 
                        out levelsRead, out valuesRead, out nullCount, out var returnValue));
                    GC.KeepAlive(Handle);
                    return returnValue;
                }

                throw new NotSupportedException($"type {type} is not supported");
            }
        }

        public override long Skip(long numRowsToSkip)
        {
            var type = typeof(TValue);

            if (type == typeof(bool))
            {
                return ExceptionInfo.Return<long, long>(Handle, numRowsToSkip, TypedColumnReader_Skip_Bool);
            }

            if (type == typeof(int))
            {
                return ExceptionInfo.Return<long, long>(Handle, numRowsToSkip, TypedColumnReader_Skip_Int32);
            }

            if (type == typeof(long))
            {
                return ExceptionInfo.Return<long, long>(Handle, numRowsToSkip, TypedColumnReader_Skip_Int64);
            }

            if (type == typeof(Int96))
            {
                return ExceptionInfo.Return<long, long>(Handle, numRowsToSkip, TypedColumnReader_Skip_Int96);
            }

            if (type == typeof(float))
            {
                return ExceptionInfo.Return<long, long>(Handle, numRowsToSkip, TypedColumnReader_Skip_Float);
            }

            if (type == typeof(double))
            {
                return ExceptionInfo.Return<long, long>(Handle, numRowsToSkip, TypedColumnReader_Skip_Double);
            }

            if (type == typeof(ByteArray))
            {
                return ExceptionInfo.Return<long, long>(Handle, numRowsToSkip, TypedColumnReader_Skip_ByteArray);
            }

            if (type == typeof(FixedLenByteArray))
            {
                return ExceptionInfo.Return<long, long>(Handle, numRowsToSkip, TypedColumnReader_Skip_FixedLenByteArray);
            }

            throw new NotSupportedException($"type {type} is not supported");
        }
    }
}