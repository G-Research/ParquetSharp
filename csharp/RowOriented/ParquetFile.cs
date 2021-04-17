using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using ParquetSharp.IO;

namespace ParquetSharp.RowOriented
{
    /// <summary>
    /// Static factory for creating row-oriented Parquet readers and writers.
    /// This is a higher-level API not part of apache-parquet-cpp.
    /// </summary>
    public static class ParquetFile
    {
        public static Action<Expression> OnReadExpressionCreated;
        public static Action<Expression> OnWriteExpressionCreated;

        /// <summary>
        /// Create a row-oriented reader from a file.
        /// </summary>
        public static ParquetRowReader<TTuple> CreateRowReader<TTuple>(string path)
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var readDelegate = GetOrCreateReadDelegate<TTuple>(fields);
            return new ParquetRowReader<TTuple>(path, readDelegate, fields);
        }

        public static ParquetRowReader<TTuple> CreateRowReader<TTuple>(string path, ReaderProperties readerProperties)
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var readDelegate = GetOrCreateReadDelegate<TTuple>(fields);
            return new ParquetRowReader<TTuple>(path, readerProperties, readDelegate, fields);
        }

        /// <summary>
        /// Create a row-oriented reader from an input stream.
        /// </summary>
        public static ParquetRowReader<TTuple> CreateRowReader<TTuple>(RandomAccessFile randomAccessFile)
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var readDelegate = GetOrCreateReadDelegate<TTuple>(fields);
            return new ParquetRowReader<TTuple>(randomAccessFile, readDelegate, fields);
        }

        public static ParquetRowReader<TTuple> CreateRowReader<TTuple>(RandomAccessFile randomAccessFile, ReaderProperties readerProperties)
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));
            var readDelegate = GetOrCreateReadDelegate<TTuple>(fields);
            return new ParquetRowReader<TTuple>(randomAccessFile, readerProperties, readDelegate, fields);
        }

        /// <summary>
        /// Create a row-oriented writer to a file. By default, the column names are reflected from the tuple public fields and properties.
        /// </summary>
        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            string path,
            string[] columnNames = null,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(path, columns, compression, keyValueMetadata, writeDelegate);
        }

        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            string path,
            WriterProperties writerProperties,
            string[] columnNames = null,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(path, columns, writerProperties, keyValueMetadata, writeDelegate);
        }

        /// <summary>
        /// Create a row-oriented writer to an output stream. By default, the column names are reflected from the tuple public fields and properties.
        /// </summary>
        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            OutputStream outputStream,
            string[] columnNames = null,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(outputStream, columns, compression, keyValueMetadata, writeDelegate);
        }

        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            OutputStream outputStream,
            WriterProperties writerProperties,
            string[] columnNames = null,
            IReadOnlyDictionary<string, string> keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(outputStream, columns, writerProperties, keyValueMetadata, writeDelegate);
        }

        private static ParquetRowReader<TTuple>.ReadAction GetOrCreateReadDelegate<TTuple>((string name, string mappedColumn, Type type, MemberInfo info)[] fields)
        {
            return (ParquetRowReader<TTuple>.ReadAction) ReadDelegatesCache.GetOrAdd(typeof(TTuple), k => CreateReadDelegate<TTuple>(fields));
        }

        private static (Column[] columns, ParquetRowWriter<TTuple>.WriteAction writeDelegate) GetOrCreateWriteDelegate<TTuple>(string[] columnNames)
        {
            var (columns, writeDelegate) = WriteDelegates.GetOrAdd(typeof(TTuple), k => CreateWriteDelegate<TTuple>());
            if (columnNames != null)
            {
                if (columnNames.Length != columns.Length)
                {
                    throw new ArgumentException("the length of column names does not match the number of public fields and properties", nameof(columnNames));
                }

                columns = columns.Select((c, i) => new Column(c.LogicalSystemType, columnNames[i], c.LogicalTypeOverride, c.Length)).ToArray();
            }

            return (columns, (ParquetRowWriter<TTuple>.WriteAction) writeDelegate);
        }

        /// <summary>
        /// Returns a delegate to read rows from individual Parquet columns.
        /// </summary>
        private static ParquetRowReader<TTuple>.ReadAction CreateReadDelegate<TTuple>(
            (string name, string mappedColumn, Type type, MemberInfo info)[] fields)
        {
            // Parameters
            var reader = Expression.Parameter(typeof(ParquetRowReader<TTuple>), "reader");
            var tuples = Expression.Parameter(typeof(TTuple[]), "tuples");
            var length = Expression.Parameter(typeof(int), "length");

            // Buffers.
            var buffers = new List<ParameterExpression>();
            var bufferAssigns = new List<Expression>();
            // Read the columns from Parquet and populate the buffers.
            var reads = new List<MethodCallExpression>();
            // Loop over the tuples, constructing them from the column buffers.
            var index = Expression.Variable(typeof(int), "index");
            var loop = For(index, Expression.Constant(0), Expression.NotEqual(index, length),
                Expression.PreIncrementAssign(index),
                Expression.Assign(
                    Expression.ArrayAccess(tuples, index),
                    ConstructType(typeof(TTuple), fields)
                )
            );

            Expression CreateBuffer((string name, string mappedColumn, Type type, MemberInfo info) field)
            {
                var expr = Expression.Variable(field.type.MakeArrayType(), $"buffer_{field.name}");
                buffers.Add(expr);
                bufferAssigns.Add(Expression.Assign(expr, Expression.NewArrayBounds(field.type, length)));
                reads.Add(Expression.Call(reader, GetReadMethod<TTuple>(field.type),
                    Expression.Constant(buffers.Count - 1), expr, length));
                return expr;
            }

            Expression ConstructType(Type type,
                (string name, string mappedColumn, Type type, MemberInfo info)[] fields2)
            {
                // Use constructor or the property setters.
                var ctorInfo = type.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null,
                    fields2.Select(f => f.type).ToArray(), null);

                return ctorInfo is null
                    ? (Expression) Expression.MemberInit(Expression.New(type), FieldBindings())
                    : Expression.New(ctorInfo, FieldExpressions());

                Expression FieldExpression((string name, string mappedColumn, Type type, MemberInfo info) field)
                {
                    if (!Column.IsSupported(field.type))
                    {
                        return ConstructType(field.type, GetFieldsAndProperties(field.type));
                    }

                    return Expression.ArrayAccess(CreateBuffer(field), index);
                }

                IEnumerable<MemberBinding> FieldBindings()
                {
                    return fields2.Select(field => Expression.Bind(field.info, FieldExpression(field)));
                }

                IEnumerable<Expression> FieldExpressions()
                {
                    return fields2.Select(FieldExpression);
                }
            }

            var body = Expression.Block(buffers.ToArray(), bufferAssigns.Concat(reads).Concat(new[] {loop}));
            var lambda = Expression.Lambda<ParquetRowReader<TTuple>.ReadAction>(body, reader, tuples, length);
            OnReadExpressionCreated?.Invoke(lambda);
            return lambda.Compile();
        }

        /// <summary>
        /// Return a delegate to write rows to individual Parquet columns, as well the column types and names.
        /// </summary>
        private static (Column[] columns, ParquetRowWriter<TTuple>.WriteAction writeDelegate) CreateWriteDelegate<TTuple>()
        {
            var columns = new List<Column>();

            // Parameters
            var writer = Expression.Parameter(typeof(ParquetRowWriter<TTuple>), "writer");
            var tuples = Expression.Parameter(typeof(TTuple[]), "tuples");
            var length = Expression.Parameter(typeof(int), "length");

            var index = Expression.Variable(typeof(int), "index");
            IEnumerable<Expression> ExpressionsForType(Type type, Func<Expression> value)
            {
                return GetFieldsAndProperties(type).SelectMany(field =>
                {
                    if (!Column.IsSupported(field.type))
                    {
                        return ExpressionsForType(field.type, () => Expression.PropertyOrField(value(), field.name));
                    }

                    columns.Add(GetColumn(field));

                    // Column buffer
                    var bufferType = field.type.MakeArrayType();
                    var buffer = Expression.Variable(bufferType, $"buffer_{field.name}");
                    var bufferAssign = Expression.Assign(buffer, Expression.NewArrayBounds(field.type, length));
                    var bufferReset = Expression.Assign(buffer, Expression.Constant(null, bufferType));

                    // Loop over the tuples and fill the current column buffer.
                    var loop = For(index, Expression.Constant(0), Expression.NotEqual(index, length),
                        Expression.PreIncrementAssign(index),
                        Expression.Assign( // buffer[i] = tuples[i].field
                            Expression.ArrayAccess(buffer, index),
                            Expression.PropertyOrField(value(), field.name)
                        )
                    );

                    // Write the buffer to Parquet.
                    var writeCall = Expression.Call(writer, GetWriteMethod<TTuple>(buffer.Type.GetElementType()),
                        buffer, length);

                    return new[]
                    {
                        (Expression) Expression.Block(
                            new[] {buffer},
                            bufferAssign,
                            loop,
                            writeCall,
                            bufferReset)
                    };
                });
            }
            var columnBodies = ExpressionsForType(typeof(TTuple), () => Expression.ArrayAccess(tuples, index));

            var body = Expression.Block(new []{index}, columnBodies);
            var lambda = Expression.Lambda<ParquetRowWriter<TTuple>.WriteAction>(body, writer, tuples, length);
            OnWriteExpressionCreated?.Invoke(lambda);
            return (columns.ToArray(), lambda.Compile());
        }

        private static MethodInfo GetReadMethod<TTuple>(Type type)
        {
            var genericMethod = typeof(ParquetRowReader<TTuple>).GetMethod(nameof(ParquetRowReader<TTuple>.ReadColumn), BindingFlags.NonPublic | BindingFlags.Instance);
            if (genericMethod == null)
            {
                throw new ArgumentException("could not find a ParquetReader generic read method");
            }

            return genericMethod.MakeGenericMethod(type);
        }

        private static MethodInfo GetWriteMethod<TTuple>(Type type)
        {
            var genericMethod = typeof(ParquetRowWriter<TTuple>).GetMethod(nameof(ParquetRowWriter<TTuple>.WriteColumn), BindingFlags.NonPublic | BindingFlags.Instance);
            if (genericMethod == null)
            {
                throw new ArgumentException("could not find a ParquetWriter generic writer method");
            }

            return genericMethod.MakeGenericMethod(type);
        }

        private static Expression For(
            ParameterExpression loopVar,
            Expression initValue, Expression condition, Expression increment,
            Expression loopContent)
        {
            var initAssign = Expression.Assign(loopVar, initValue);
            var breakLabel = Expression.Label("LoopBreak");

            return Expression.Block(new[] { loopVar },
                initAssign,
                Expression.Loop(
                    Expression.IfThenElse(
                        condition,
                        Expression.Block(
                            loopContent,
                            increment),
                        Expression.Break(breakLabel)),
                    breakLabel)
            );
        }

        private static (string name, string mappedColumn, Type type, MemberInfo info)[] GetFieldsAndProperties(Type type)
        {
            var list = new List<(string name, string mappedColumn, Type type, MemberInfo info)>();
            var flags = BindingFlags.Public | BindingFlags.Instance;

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTuple<,,,,,,,>))
            {
                throw new ArgumentException("System.ValueTuple TTuple types beyond 7 in length are not supported");
            }

            foreach (var field in type.GetFields(flags))
            {
                var mappedColumn = field.GetCustomAttribute<MapToColumnAttribute>()?.ColumnName;
                list.Add((field.Name, mappedColumn, field.FieldType, field));
            }

            foreach (var property in type.GetProperties(flags))
            {
                var mappedColumn = property.GetCustomAttribute<MapToColumnAttribute>()?.ColumnName;
                list.Add((property.Name, mappedColumn, property.PropertyType, property));
            }

            // The order in which fields are processed is important given that when a tuple type is used in
            // ParquetFile.CreateRowWriter<TTuple>() with an array of column names it is expected that
            // the resulting parquet file correctly maps the name to the appropriate column type.
            //
            // However, neither Type.GetFields() nor Type.GetProperties() guarantee the order in which they return
            // fields or properties - importantly this means that they will not always be returned in
            // declaration order, not even for ValueTuples. The accepted means of returning fields and
            // properties in declaration order is to sort by MemberInfo.MetadataToken. This is done after
            // both the fields and properties have been gathered for greatest consistency.
            //
            // See https://stackoverflow.com/questions/8067493/if-getfields-doesnt-guarantee-order-how-does-layoutkind-sequential-work and
            // https://github.com/dotnet/corefx/issues/14606 for more detail.
            //
            // Note that most of the time GetFields() and GetProperties() _do_ return in declaration order and the times when they don't
            // are determined at runtime and not by the type. As a result it is pretty much impossible to cover this with a unit test. Hence this
            // rather long comment aimed at avoiding accidental removal!
            return list.OrderBy(x => x.info.MetadataToken).ToArray();
        }

        private static Column GetColumn((string name, string mappedColumn, Type type, MemberInfo info) field)
        {
            var isDecimal = field.type == typeof(decimal) || field.type == typeof(decimal?);
            var decimalScale = field.info.GetCustomAttributes(typeof(ParquetDecimalScaleAttribute))
                .Cast<ParquetDecimalScaleAttribute>()
                .SingleOrDefault();

            if (!isDecimal && decimalScale != null)
            {
                throw new ArgumentException($"field '{field.name}' has a {nameof(ParquetDecimalScaleAttribute)} despite not being a decimal type");
            }

            if (isDecimal && decimalScale == null)
            {
                throw new ArgumentException($"field '{field.name}' has no {nameof(ParquetDecimalScaleAttribute)} despite being a decimal type");
            }

            return new Column(field.type, field.mappedColumn ?? field.name, isDecimal ? LogicalType.Decimal(29, decimalScale.Scale) : null);
        }

        private static readonly ConcurrentDictionary<Type, Delegate> ReadDelegatesCache =
            new ConcurrentDictionary<Type, Delegate>();

        private static readonly ConcurrentDictionary<Type, (Column[] columns, Delegate writeDelegate)> WriteDelegates =
            new ConcurrentDictionary<Type, (Column[] columns, Delegate writeDelegate)>();
    }
}
