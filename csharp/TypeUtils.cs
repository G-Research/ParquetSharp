using System;
using System.Linq;

namespace ParquetSharp
{
    internal static class TypeUtils
    {
        public static bool IsNullable(Type type, out Type inner)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nullable<>))
            {
                inner = null!;
                return false;
            }
            inner = type.GetGenericArguments().Single();
            return true;
        }

        public static bool IsNested(Type type, out Type inner)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nested<>))
            {
                inner = null!;
                return false;
            }
            inner = type.GetGenericArguments().Single();
            return true;
        }

        public static bool IsNullableNested(Type type, out Type inner)
        {
            if (IsNullable(type, out var nullableInner) && IsNested(nullableInner, out var nestedInner))
            {
                inner = nestedInner;
                return true;
            }
            inner = null!;
            return false;
        }

        /// <summary>
        /// Whether to use the <see cref="Decimal128"/> type for conversion between decimal values
        /// and fixed length byte array data.
        /// </summary>
        public static unsafe bool UseDecimal128(ColumnDescriptor columnDescriptor)
        {
            // Even if the type length matches Decimal128, we want to use DecimalConverter for higher
            // precision than decimal supports so that we check for overflow and don't silently read invalid data.
            return columnDescriptor.TypeLength == sizeof(Decimal128) && columnDescriptor.TypePrecision <= 29;
        }
    }
}
