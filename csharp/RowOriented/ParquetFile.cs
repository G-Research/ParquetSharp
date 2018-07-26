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
            var readDelegate = GetOrCreateReadDelegate<TTuple>();
            return new ParquetRowReader<TTuple>(path, readDelegate);
        }

        /// <summary>
        /// Create a row-oriented reader from an input stream.
        /// </summary>
        public static ParquetRowReader<TTuple> CreateRowReader<TTuple>(InputStream inputStream)
        {
            var readDelegate = GetOrCreateReadDelegate<TTuple>();
            return new ParquetRowReader<TTuple>(inputStream, readDelegate);
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
            return new ParquetRowWriter<TTuple>(path, columns, writeDelegate);
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
            return new ParquetRowWriter<TTuple>(outputStream, columns, writeDelegate);
        }

        private static ParquetRowReader<TTuple>.ReadAction GetOrCreateReadDelegate<TTuple>()
        {
            return (ParquetRowReader<TTuple>.ReadAction) ReadDelegatesCache.GetOrAdd(typeof(TTuple), k => CreateReadDelegate<TTuple>());
        }

        private static (Column[] columns, ParquetRowWriter<TTuple>.WriteAction writeDelegate) GetOrCreateWriteDelegate<TTuple>(string[] columnNames)
        {
            var (columns, writeDelegate) = WriteDelegates.GetOrAdd(typeof(TTuple), k => CreateWriteDelegate<TTuple>());
            if (columnNames != null)
            {
                if (columnNames.Length != columns.Length)
                {
                    throw new ArgumentException("the length of column names does not mach the number of public fields and properties", nameof(columnNames));
                }

                columns = columns.Select((c, i) => new Column(c.LogicalSystemType, columnNames[i])).ToArray();
            }

            return (columns, (ParquetRowWriter<TTuple>.WriteAction) writeDelegate);
        }

        /// <summary>
        /// Returns a delegate to read rows from individual Parquet columns.
        /// </summary>
        private static ParquetRowReader<TTuple>.ReadAction CreateReadDelegate<TTuple>()
        {
            var fields = GetFieldsAndProperties(typeof(TTuple), BindingFlags.Public | BindingFlags.Instance).ToArray();

            // Parameters
            var reader = Expression.Parameter(typeof(ParquetRowReader<TTuple>), "reader");
            var tuples = Expression.Parameter(typeof(TTuple[]), "tuples");
            var length = Expression.Parameter(typeof(int), "length");

            // Use constructor or the property setters.
            var ctor = typeof(TTuple).GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, fields.Select(f => f.type).ToArray(), null);

            // Buffers.
            var buffers = fields.Select(f => Expression.Variable(f.type.MakeArrayType(), $"buffer_{f.name}")).ToArray();
            var bufferAssigns = fields.Select((f, i) => (Expression) Expression.Assign(buffers[i], Expression.NewArrayBounds(f.type, length))).ToArray();

            // Read the columns from Parquet and populate the buffers.
            var reads = buffers.Select((buffer, i) => Expression.Call(reader, GetReadMethod<TTuple>(fields[i].type), Expression.Constant(i), buffer, length)).ToArray();

            // Loop over the tuples, constructing them from the column buffers.
            var index = Expression.Variable(typeof(int), "index");
            var loop = For(index, Expression.Constant(0), Expression.NotEqual(index, length), Expression.PreIncrementAssign(index),
                Expression.Assign(
                    Expression.ArrayAccess(tuples, index),
                    ctor == null
                        ? Expression.MemberInit(Expression.New(typeof(TTuple)), fields.Select((f, i) => Expression.Bind(f.info, Expression.ArrayAccess(buffers[i], index))))
                        : (Expression) Expression.New(ctor, fields.Select((f, i) => (Expression) Expression.ArrayAccess(buffers[i], index)))
                )
            );

            var body = Expression.Block(buffers, bufferAssigns.Concat(reads).Concat(new[] {loop}));
            var lambda = Expression.Lambda<ParquetRowReader<TTuple>.ReadAction>(body, reader, tuples, length);
            OnReadExpressionCreated?.Invoke(lambda);
            return lambda.Compile();
        }

        /// <summary>
        /// Return a delegate to write rows to individual Parquet columns, as well the column types and names.
        /// </summary>
        private static (Column[] columns, ParquetRowWriter<TTuple>.WriteAction writeDelegate) CreateWriteDelegate<TTuple>()
        {
            var fields = GetFieldsAndProperties(typeof(TTuple), BindingFlags.Public | BindingFlags.Instance).ToArray();
            var columns = fields.Select(f => new Column(f.type, f.name)).ToArray();

            // Parameters
            var writer = Expression.Parameter(typeof(ParquetRowWriter<TTuple>), "writer");
            var tuples = Expression.Parameter(typeof(TTuple[]), "tuples");
            var length = Expression.Parameter(typeof(int), "length");

            var columnBodies = fields.Select(f =>
            {
                // Column buffer
                var bufferType = f.type.MakeArrayType();
                var buffer = Expression.Variable(bufferType, $"buffer_{f.name}");
                var bufferAssign = Expression.Assign(buffer, Expression.NewArrayBounds(f.type, length));
                var bufferReset = Expression.Assign(buffer, Expression.Constant(null, bufferType));

                // Loop over the tuples and fill the current column buffer.
                var index = Expression.Variable(typeof(int), "index");
                var loop = For(index, Expression.Constant(0), Expression.NotEqual(index, length), Expression.PreIncrementAssign(index),
                    Expression.Assign(
                        Expression.ArrayAccess(buffer, index),
                        Expression.PropertyOrField(Expression.ArrayAccess(tuples, index), f.name)
                    )
                );

                // Write the buffer to Parquet.
                var writeCall = Expression.Call(writer, GetWriteMethod<TTuple>(buffer.Type.GetElementType()), buffer, length);

                return Expression.Block(
                    new[] {buffer, index},
                    bufferAssign,
                    loop,
                    writeCall,
                    bufferReset);
            });

            var body = Expression.Block(columnBodies);
            var lambda = Expression.Lambda<ParquetRowWriter<TTuple>.WriteAction>(body, writer, tuples, length);
            OnWriteExpressionCreated?.Invoke(lambda);
            return (columns, lambda.Compile());
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

        private static (string name, Type type, MemberInfo info)[] GetFieldsAndProperties(Type type, BindingFlags flags)
        {
            var list = new List<(string name, Type type, MemberInfo info)>();

            foreach (var field in type.GetFields(flags))
            {
                list.Add((field.Name, field.FieldType, field));
            }

            foreach (var property in type.GetProperties(flags))
            {
                list.Add((property.Name, property.PropertyType, property));
            }

            return list.ToArray();
        }

        private static readonly ConcurrentDictionary<Type, Delegate> ReadDelegatesCache = 
            new ConcurrentDictionary<Type, Delegate>();

        private static readonly ConcurrentDictionary<Type, (Column[] columns, Delegate writeDelegate)> WriteDelegates = 
            new ConcurrentDictionary<Type, (Column[] columns, Delegate writeDelegate)>();
    }
}
