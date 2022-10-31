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
    }
}
