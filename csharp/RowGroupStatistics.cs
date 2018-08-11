using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public abstract class RowGroupStatistics : IDisposable
    {
        internal static RowGroupStatistics Create(IntPtr handle)
        {
            if (handle == IntPtr.Zero)
            {
                return null;
            }

            var parquetHandle = new ParquetHandle(handle, RowGroupStatistics_Free);

            try
            {
                var type = ExceptionInfo.Return<PhysicalType>(handle, RowGroupStatistics_Physical_Type);

                switch (type)
                {
                    case PhysicalType.Boolean:
                        return new RowGroupStatistics<bool>(parquetHandle);
                    case PhysicalType.Int32:
                        return new RowGroupStatistics<int>(parquetHandle);
                    case PhysicalType.Int64:
                        return new RowGroupStatistics<long>(parquetHandle);
                    case PhysicalType.Int96:
                        return new RowGroupStatistics<Int96>(parquetHandle);
                    case PhysicalType.Float:
                        return new RowGroupStatistics<float>(parquetHandle);
                    case PhysicalType.Double:
                        return new RowGroupStatistics<double>(parquetHandle);
                    case PhysicalType.ByteArray:
                        return new RowGroupStatistics<ByteArray>(parquetHandle);
                    case PhysicalType.FixedLenByteArray:
                        return new RowGroupStatistics<FixedLenByteArray>(parquetHandle);
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

        internal RowGroupStatistics(ParquetHandle handle)
        {
            Handle = handle;
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public long DistinctCount => ExceptionInfo.Return<long>(Handle, RowGroupStatistics_Distinct_Count);
        public bool HasMinMax => ExceptionInfo.Return<bool>(Handle, RowGroupStatistics_HasMinMax);
        public long NullCount => ExceptionInfo.Return<long>(Handle, RowGroupStatistics_Null_Count);
        public long NumValues => ExceptionInfo.Return<long>(Handle, RowGroupStatistics_Num_Values);
        public PhysicalType PhysicalType => ExceptionInfo.Return<PhysicalType>(Handle, RowGroupStatistics_Physical_Type);

        public abstract object MinUntyped { get; }
        public abstract object MaxUntyped { get; }

        [DllImport(ParquetDll.Name)]
        private static extern void RowGroupStatistics_Free(IntPtr statistics);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupStatistics_Distinct_Count(IntPtr statistics, out long distinctCount);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupStatistics_HasMinMax(IntPtr statistics, out bool hasMinMax);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupStatistics_Null_Count(IntPtr statistics, out long nullCount);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupStatistics_Num_Values(IntPtr statistics, out long numValues);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr RowGroupStatistics_Physical_Type(IntPtr statistics, out PhysicalType physicalType);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Min_Bool(IntPtr statistics, out bool min);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Min_Int32(IntPtr statistics, out int min);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Min_Int64(IntPtr statistics, out long min);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Min_Int96(IntPtr statistics, out Int96 min);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Min_Float(IntPtr statistics, out float min);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Min_Double(IntPtr statistics, out double min);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Min_ByteArray(IntPtr statistics, out ByteArray min);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Min_FLBA(IntPtr statistics, out FixedLenByteArray min);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Max_Bool(IntPtr statistics, out bool max);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Max_Int32(IntPtr statistics, out int max);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Max_Int64(IntPtr statistics, out long max);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Max_Int96(IntPtr statistics, out Int96 max);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Max_Float(IntPtr statistics, out float max);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Max_Double(IntPtr statistics, out double max);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Max_ByteArray(IntPtr statistics, out ByteArray max);

        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedRowGroupStatistics_Max_FLBA(IntPtr statistics, out FixedLenByteArray max);

        internal readonly ParquetHandle Handle;
    }

    public sealed class RowGroupStatistics<TValue> : RowGroupStatistics where TValue : unmanaged
    {
        internal RowGroupStatistics(ParquetHandle handle)
            : base(handle)
        {
        }

        public override object MinUntyped => Min;
        public override object MaxUntyped => Max;

        public TValue Min
        {
            get
            {
                var type = typeof(TValue);

                if (type == typeof(bool))
                {
                    return (TValue) (object) ExceptionInfo.Return<bool>(Handle, TypedRowGroupStatistics_Min_Bool);
                }

                if (type == typeof(int))
                {
                    return (TValue) (object) ExceptionInfo.Return<int>(Handle, TypedRowGroupStatistics_Min_Int32);
                }

                if (type == typeof(long))
                {
                    return (TValue) (object) ExceptionInfo.Return<long>(Handle, TypedRowGroupStatistics_Min_Int64);
                }

                if (type == typeof(Int96))
                {
                    return (TValue) (object) ExceptionInfo.Return<Int96>(Handle, TypedRowGroupStatistics_Min_Int96);
                }

                if (type == typeof(float))
                {
                    return (TValue) (object) ExceptionInfo.Return<float>(Handle, TypedRowGroupStatistics_Min_Float);
                }

                if (type == typeof(double))
                {
                    return (TValue) (object) ExceptionInfo.Return<double>(Handle, TypedRowGroupStatistics_Min_Double);
                }

                if (type == typeof(ByteArray))
                {
                    return (TValue) (object) ExceptionInfo.Return<ByteArray>(Handle, TypedRowGroupStatistics_Min_ByteArray);
                }

                if (type == typeof(FixedLenByteArray))
                {
                    return (TValue) (object) ExceptionInfo.Return<FixedLenByteArray>(Handle, TypedRowGroupStatistics_Min_FLBA);
                }

                throw new NotSupportedException($"type {type} is not supported");
            }
        }

        public TValue Max
        {
            get
            {
                var type = typeof(TValue);

                if (type == typeof(bool))
                {
                    return (TValue) (object) ExceptionInfo.Return<bool>(Handle, TypedRowGroupStatistics_Max_Bool);
                }

                if (type == typeof(int))
                {
                    return (TValue) (object) ExceptionInfo.Return<int>(Handle, TypedRowGroupStatistics_Max_Int32);
                }

                if (type == typeof(long))
                {
                    return (TValue) (object) ExceptionInfo.Return<long>(Handle, TypedRowGroupStatistics_Max_Int64);
                }

                if (type == typeof(Int96))
                {
                    return (TValue) (object) ExceptionInfo.Return<Int96>(Handle, TypedRowGroupStatistics_Max_Int96);
                }

                if (type == typeof(float))
                {
                    return (TValue) (object) ExceptionInfo.Return<float>(Handle, TypedRowGroupStatistics_Max_Float);
                }

                if (type == typeof(double))
                {
                    return (TValue) (object) ExceptionInfo.Return<double>(Handle, TypedRowGroupStatistics_Max_Double);
                }

                if (type == typeof(ByteArray))
                {
                    return (TValue) (object) ExceptionInfo.Return<ByteArray>(Handle, TypedRowGroupStatistics_Max_ByteArray);
                }

                if (type == typeof(FixedLenByteArray))
                {
                    return (TValue) (object) ExceptionInfo.Return<FixedLenByteArray>(Handle, TypedRowGroupStatistics_Max_FLBA);
                }

                throw new NotSupportedException($"type {type} is not supported");
            }
        }
    }
}
