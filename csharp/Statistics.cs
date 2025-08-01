﻿using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public abstract class Statistics : IDisposable
    {
        internal static Statistics? Create(IntPtr handle)
        {
            if (handle == IntPtr.Zero)
            {
                return null;
            }

            var parquetHandle = new ParquetHandle(handle, Statistics_Free);

            try
            {
                var type = ExceptionInfo.Return<PhysicalType>(handle, Statistics_Physical_Type);

                return type switch
                {
                    PhysicalType.Boolean => new Statistics<bool>(parquetHandle),
                    PhysicalType.Int32 => new Statistics<int>(parquetHandle),
                    PhysicalType.Int64 => new Statistics<long>(parquetHandle),
                    PhysicalType.Int96 => new Statistics<Int96>(parquetHandle),
                    PhysicalType.Float => new Statistics<float>(parquetHandle),
                    PhysicalType.Double => new Statistics<double>(parquetHandle),
                    PhysicalType.ByteArray => new Statistics<ByteArray>(parquetHandle),
                    PhysicalType.FixedLenByteArray => new Statistics<FixedLenByteArray>(parquetHandle),
                    _ => throw new NotSupportedException($"Physical type {type} is not supported")
                };
            }

            catch
            {
                parquetHandle.Dispose();
                throw;
            }
        }

        internal Statistics(ParquetHandle handle)
        {
            Handle = handle;
        }

        public void Dispose()
        {
            Handle.Dispose();
        }

        public long DistinctCount => ExceptionInfo.Return<long>(Handle, Statistics_Distinct_Count);
        public bool HasMinMax => ExceptionInfo.Return<bool>(Handle, Statistics_HasMinMax);
        public long NullCount => ExceptionInfo.Return<long>(Handle, Statistics_Null_Count);
        public long NumValues => ExceptionInfo.Return<long>(Handle, Statistics_Num_Values);
        public PhysicalType PhysicalType => ExceptionInfo.Return<PhysicalType>(Handle, Statistics_Physical_Type);

        public abstract object MinUntyped { get; }
        public abstract object MaxUntyped { get; }

        [DllImport(ParquetDll.Name)]
        private static extern void Statistics_Free(IntPtr statistics);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Statistics_Distinct_Count(IntPtr statistics, out long distinctCount);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Statistics_HasMinMax(IntPtr statistics, [MarshalAs(UnmanagedType.I1)] out bool hasMinMax);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Statistics_Null_Count(IntPtr statistics, out long nullCount);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Statistics_Num_Values(IntPtr statistics, out long numValues);

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr Statistics_Physical_Type(IntPtr statistics, out PhysicalType physicalType);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Min_Bool(IntPtr statistics, [MarshalAs(UnmanagedType.I1)] out bool min);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Min_Int32(IntPtr statistics, out int min);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Min_Int64(IntPtr statistics, out long min);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Min_Int96(IntPtr statistics, out Int96 min);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Min_Float(IntPtr statistics, out float min);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Min_Double(IntPtr statistics, out double min);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Min_ByteArray(IntPtr statistics, out ByteArray min);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Min_FLBA(IntPtr statistics, out FixedLenByteArray min);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Max_Bool(IntPtr statistics, [MarshalAs(UnmanagedType.I1)] out bool max);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Max_Int32(IntPtr statistics, out int max);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Max_Int64(IntPtr statistics, out long max);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Max_Int96(IntPtr statistics, out Int96 max);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Max_Float(IntPtr statistics, out float max);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Max_Double(IntPtr statistics, out double max);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Max_ByteArray(IntPtr statistics, out ByteArray max);

        /// <exclude />
        [DllImport(ParquetDll.Name)]
        protected static extern IntPtr TypedStatistics_Max_FLBA(IntPtr statistics, out FixedLenByteArray max);

        internal readonly ParquetHandle Handle;
    }

    public sealed class Statistics<TValue> : Statistics where TValue : unmanaged
    {
        internal Statistics(ParquetHandle handle)
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
                    return (TValue) (object) ExceptionInfo.Return<bool>(Handle, TypedStatistics_Min_Bool);
                }

                if (type == typeof(int))
                {
                    return (TValue) (object) ExceptionInfo.Return<int>(Handle, TypedStatistics_Min_Int32);
                }

                if (type == typeof(long))
                {
                    return (TValue) (object) ExceptionInfo.Return<long>(Handle, TypedStatistics_Min_Int64);
                }

                if (type == typeof(Int96))
                {
                    return (TValue) (object) ExceptionInfo.Return<Int96>(Handle, TypedStatistics_Min_Int96);
                }

                if (type == typeof(float))
                {
                    return (TValue) (object) ExceptionInfo.Return<float>(Handle, TypedStatistics_Min_Float);
                }

                if (type == typeof(double))
                {
                    return (TValue) (object) ExceptionInfo.Return<double>(Handle, TypedStatistics_Min_Double);
                }

                if (type == typeof(ByteArray))
                {
                    return (TValue) (object) ExceptionInfo.Return<ByteArray>(Handle, TypedStatistics_Min_ByteArray);
                }

                if (type == typeof(FixedLenByteArray))
                {
                    return (TValue) (object) ExceptionInfo.Return<FixedLenByteArray>(Handle, TypedStatistics_Min_FLBA);
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
                    return (TValue) (object) ExceptionInfo.Return<bool>(Handle, TypedStatistics_Max_Bool);
                }

                if (type == typeof(int))
                {
                    return (TValue) (object) ExceptionInfo.Return<int>(Handle, TypedStatistics_Max_Int32);
                }

                if (type == typeof(long))
                {
                    return (TValue) (object) ExceptionInfo.Return<long>(Handle, TypedStatistics_Max_Int64);
                }

                if (type == typeof(Int96))
                {
                    return (TValue) (object) ExceptionInfo.Return<Int96>(Handle, TypedStatistics_Max_Int96);
                }

                if (type == typeof(float))
                {
                    return (TValue) (object) ExceptionInfo.Return<float>(Handle, TypedStatistics_Max_Float);
                }

                if (type == typeof(double))
                {
                    return (TValue) (object) ExceptionInfo.Return<double>(Handle, TypedStatistics_Max_Double);
                }

                if (type == typeof(ByteArray))
                {
                    return (TValue) (object) ExceptionInfo.Return<ByteArray>(Handle, TypedStatistics_Max_ByteArray);
                }

                if (type == typeof(FixedLenByteArray))
                {
                    return (TValue) (object) ExceptionInfo.Return<FixedLenByteArray>(Handle, TypedStatistics_Max_FLBA);
                }

                throw new NotSupportedException($"type {type} is not supported");
            }
        }
    }
}
