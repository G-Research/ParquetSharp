using System;
using System.Collections.Generic;
using System.Text;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace ParquetSharp.Benchmark
{
    internal sealed class ResultColumn : IColumn
    {
        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            throw new NotImplementedException();
        }

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            return GetValue(summary, benchmarkCase);
        }

        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase)
        {
            throw new NotImplementedException();
        }

        public bool IsAvailable(Summary summary)
        {
            throw new NotImplementedException();
        }

        public string Id { get; }
        public string ColumnName { get; }
        public bool AlwaysShow { get; }
        public ColumnCategory Category { get; }
        public int PriorityInCategory { get; }
        public bool IsNumeric { get; }
        public UnitType UnitType { get; }
        public string Legend { get; }
    }
}
