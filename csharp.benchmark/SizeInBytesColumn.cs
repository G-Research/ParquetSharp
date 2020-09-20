using System;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace ParquetSharp.Benchmark
{
    internal sealed class SizeInBytesColumn : IColumn
    {
        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            var type = benchmarkCase.Descriptor.Type;
            var method = benchmarkCase.Descriptor.WorkloadMethod;

            if (method.ReturnType != typeof(long))
            {
                return "";
            }

            var instance = Activator.CreateInstance(type);
            var result = method.Invoke(instance, new object[0]);

            // ReSharper disable once PossibleNullReferenceException
            return ((long) result).ToString("N0");
        }

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            return GetValue(summary, benchmarkCase);
        }

        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase)
        {
            return true;
        }

        public bool IsAvailable(Summary summary)
        {
            return true;
        }

        public string Id => "Size";
        public string ColumnName => "Size (Bytes)";
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 0;
        public bool IsNumeric => true;
        public UnitType UnitType => UnitType.Size;
        public string Legend => "Size in bytes";
    }
}
