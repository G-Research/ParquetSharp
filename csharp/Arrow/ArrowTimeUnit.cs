using System;

namespace ParquetSharp.Arrow
{
    /// <summary>
    /// Arrow TimeUnit enum, with values corresponding to those used in the C++ library.
    /// These do match the C# Arrow library enum values but there's no guarantee that will always be true.
    /// </summary>
    internal enum ArrowTimeUnit
    {
        Second = 0,
        Millisecond = 1,
        Microsecond = 2,
        Nanosecond = 3,
    }

    internal static class ArrowTimeUnitUtils
    {
        public static ArrowTimeUnit FromArrow(Apache.Arrow.Types.TimeUnit unit)
        {
            return unit switch
            {
                Apache.Arrow.Types.TimeUnit.Second => ArrowTimeUnit.Second,
                Apache.Arrow.Types.TimeUnit.Millisecond => ArrowTimeUnit.Millisecond,
                Apache.Arrow.Types.TimeUnit.Microsecond => ArrowTimeUnit.Microsecond,
                Apache.Arrow.Types.TimeUnit.Nanosecond => ArrowTimeUnit.Nanosecond,
                _ => throw new ArgumentOutOfRangeException(nameof(unit), unit, null)
            };
        }

        public static Apache.Arrow.Types.TimeUnit ToArrow(ArrowTimeUnit unit)
        {
            return unit switch
            {
                ArrowTimeUnit.Second => Apache.Arrow.Types.TimeUnit.Second,
                ArrowTimeUnit.Millisecond => Apache.Arrow.Types.TimeUnit.Millisecond,
                ArrowTimeUnit.Microsecond => Apache.Arrow.Types.TimeUnit.Microsecond,
                ArrowTimeUnit.Nanosecond => Apache.Arrow.Types.TimeUnit.Nanosecond,
                _ => throw new ArgumentOutOfRangeException(nameof(unit), unit, null)
            };
        }
    }
}
