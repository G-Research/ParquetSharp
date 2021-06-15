using System;
using System.Linq;

namespace ParquetSharp.Benchmark
{
    public abstract class FloatTimeSeriesBase
    {
        protected static Column[] CreateFloatColumns()
        {
            return new Column[]
            {
                new Column<DateTime>("DateTime", LogicalType.Timestamp(true, TimeUnit.Millis)),
                new Column<int>("ObjectId"),
                new Column<float>("Value")
            };
        }

        protected static (DateTime[] dates, int[] objectIds, float[][] values, int numRows) CreateFloatDataFrame(int numDates)
        {
            var rand = new Random(123);

            var dates = Enumerable.Range(0, numDates)
                .Select(i => new DateTime(2001, 01, 01) + TimeSpan.FromHours(i))
                .Where(d => d.DayOfWeek != DayOfWeek.Saturday && d.DayOfWeek != DayOfWeek.Sunday)
                .ToArray();

            var objectIds = Enumerable.Range(0, 10000)
                .Select(i => rand.Next())
                .Distinct()
                .OrderBy(i => i)
                .ToArray();

            var values = dates.Select(d => objectIds.Select(o => (float) rand.NextDouble()).ToArray()).ToArray();
            var numRows = values.Select(v => v.Length).Aggregate(0, (sum, l) => sum + l);

            return (dates, objectIds, values, numRows);
        }
    }
}
