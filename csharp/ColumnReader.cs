using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    /// <summary>
    /// Reader of physical Parquet values from a single column.
    /// </summary>
    public abstract class ColumnReader : IDisposable
    {
        internal static ColumnReader Create(IntPtr handle, RowGroupReader rowGroupReader, ColumnChunkMetaData columnChunkMetaData, int columnIndex)
        {
            var parquetHandle = new ParquetHandle(handle, ColumnReader_Free);

            try
            {
                var type = ExceptionInfo.Return<PhysicalType>(handle, ColumnReader_Type);

                switch (type)
                {
                    case PhysicalType.Boolean:
                        return new ColumnReader<bool>(parquetHandle, rowGroupReader, columnChunkMetaData, columnIndex);
                    case PhysicalType.Int32:
                        return new ColumnReader<int>(parquetHandle, rowGroupReader, columnChunkMetaData, columnIndex);
                    case PhysicalType.Int64:
                        return new ColumnReader<long>(parquetHandle, rowGroupReader, columnChunkMetaData, columnIndex);
                    case PhysicalType.Int96:
                        return new ColumnReader<Int96>(parquetHandle, rowGroupReader, columnChunkMetaData, columnIndex);
                    case PhysicalType.Float:
                        return new ColumnReader<float>(parquetHandle, rowGroupReader, columnChunkMetaData, columnIndex);
                    case PhysicalType.Double:
                        return new ColumnReader<double>(parquetHandle, rowGroupReader, columnChunkMetaData, columnIndex);
                    case PhysicalType.ByteArray:
                        return new ColumnReader<ByteArray>(parquetHandle, rowGroupReader, columnChunkMetaData, columnIndex);
                    case PhysicalType.FixedLenByteArray:
                        return new ColumnReader<FixedLenByteArray>(parquetHandle, rowGroupReader, columnChunkMetaData, columnIndex);
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

        internal ColumnReader(ParquetHandle handle, RowGroupReader rowGroupReader, ColumnChunkMetaData columnChunkMetaData, int columnIndex)
        {
            Handle = handle;
            RowGroupReader = rowGroupReader;
            ColumnChunkMetaData = columnChunkMetaData;
            ColumnIndex = columnIndex;
        }

        public void Dispose()
        {
            ColumnChunkMetaData.Dispose();
            Handle.Dispose();
        }

        public int ColumnIndex { get; }
        public LogicalTypeFactory LogicalTypeFactory => RowGroupReader.ParquetFileReader.LogicalTypeFactory;
        public LogicalReadConverterFactory LogicalReadConverterFactory => RowGroupReader.ParquetFileReader.LogicalReadConverterFactory;

        public ColumnDescriptor ColumnDescriptor => new(ExceptionInfo.Return<IntPtr>(Handle, ColumnReader_Descr));
        public bool HasNext => ExceptionInfo.Return<bool>(Handle, ColumnReader_HasNext);
        public PhysicalType Type => ExceptionInfo.Return<PhysicalType>(Handle, ColumnReader_Type);

        public abstract Type ElementType { get; }
        public abstract TReturn Apply<TReturn>(IColumnReaderVisitor<TReturn> visitor);

        /// <summary>
        /// Skip physical row values
        /// </summary>
        /// <param name="numRowsToSkip">number of rows to skip</param>
        /// <returns>the number of physical rows skipped</returns>
        public abstract long Skip(long numRowsToSkip);

#pragma warning disable RS0026

        public LogicalColumnReader LogicalReader(int bufferLength = 4 * 1024)
        {
            // By default we don't use nested types when reading nested data for simplicity and backwards compatibility,
            // so  users must opt in to this by using one of the typed methods,
            // or the overload that takes a useNesting parameter.
            return LogicalColumnReader.Create(this, bufferLength, elementTypeOverride: null, useNesting: false);
        }

        /// <summary>
        /// Overload for creating an untyped LogicalReader that allows specifying whether nested data should
        /// be read wrapped in the Nested type.
        /// </summary>
        public LogicalColumnReader LogicalReader(bool useNesting, int bufferLength = 4 * 1024)
        {
            return LogicalColumnReader.Create(this, bufferLength, elementTypeOverride: null, useNesting: useNesting);
        }

        public LogicalColumnReader<TElement> LogicalReader<TElement>(int bufferLength = 4 * 1024)
        {
            return LogicalColumnReader.Create<TElement>(this, bufferLength, elementTypeOverride: null);
        }

#pragma warning restore RS0026

        public LogicalColumnReader<TElement> LogicalReaderOverride<TElement>(int bufferLength = 4 * 1024)
        {
            return LogicalColumnReader.Create<TElement>(this, bufferLength, typeof(TElement));
        }

        [DllImport(ParquetDll.Name)]
        private static extern void ColumnReader_Free(IntPtr columnReader);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnReader_Descr(IntPtr columnReader, out IntPtr columnDescriptor);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnReader_HasNext(IntPtr columnReader, [MarshalAs(UnmanagedType.I1)] out bool hasNext);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ColumnReader_Type(IntPtr columnReader, out PhysicalType type);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Bool(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, bool* values,
            out long valuesRead, out long levelsRead);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Int32(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, int* values,
            out long valuesRead, out long levelsRead);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Int64(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, long* values,
            out long valuesRead, out long levelsRead);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Int96(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, Int96* values,
            out long valuesRead, out long levelsRead);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Float(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, float* values,
            out long valuesRead, out long levelsRead);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_Double(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, double* values,
            out long valuesRead, out long levelsRead);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_ByteArray(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, ByteArray* values,
            out long valuesRead, out long levelsRead);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern unsafe IntPtr TypedColumnReader_ReadBatch_FixedLenByteArray(
            IntPtr columnReader, long batchSize, short* defLevels, short* repLevels, FixedLenByteArray* values, out long valuesRead, out long levelsRead);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Bool(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Int32(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Int64(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Int96(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Float(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_Double(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_ByteArray(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedColumnReader_Skip_FixedLenByteArray(IntPtr columnReader, long numRowsToSkip, out long levelsSkipped);

        internal readonly ParquetHandle Handle;
        internal readonly RowGroupReader RowGroupReader;
        internal readonly ColumnChunkMetaData ColumnChunkMetaData;
    }

    /// <inheritdoc />
    public sealed class ColumnReader<TValue> : ColumnReader where TValue : unmanaged
    {
        internal ColumnReader(ParquetHandle handle, RowGroupReader rowGroupReader, ColumnChunkMetaData columnChunkMetaData, int columnIndex)
            : base(handle, rowGroupReader, columnChunkMetaData, columnIndex)
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
#pragma warning disable CA2265
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (values.Length < batchSize) throw new ArgumentOutOfRangeException(nameof(values), "batchSize is larger than length of values");
            if (defLevels != null && defLevels.Length < batchSize) throw new ArgumentOutOfRangeException(nameof(defLevels), "batchSize is larger than length of defLevels");
            if (repLevels != null && repLevels.Length < batchSize) throw new ArgumentOutOfRangeException(nameof(repLevels), "batchSize is larger than length of repLevels");
#pragma warning disable CA2265

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
