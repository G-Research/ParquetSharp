﻿using System;
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
        public static Action<Expression>? OnReadExpressionCreated;
        public static Action<Expression>? OnWriteExpressionCreated;

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
            string[]? columnNames = null,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(path, columns, compression, keyValueMetadata, writeDelegate);
        }

        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            string path,
            WriterProperties writerProperties,
            string[]? columnNames = null,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(path, columns, writerProperties, keyValueMetadata, writeDelegate);
        }

        /// <summary>
        /// Create a row-oriented writer to an output stream. By default, the column names are reflected from the tuple public fields and properties.
        /// </summary>
        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            OutputStream outputStream,
            string[]? columnNames = null,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(outputStream, columns, compression, keyValueMetadata, writeDelegate);
        }

        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            OutputStream outputStream,
            WriterProperties writerProperties,
            string[]? columnNames = null,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columns, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columnNames);
            return new ParquetRowWriter<TTuple>(outputStream, columns, writerProperties, keyValueMetadata, writeDelegate);
        }

        /// <summary>
        /// Create a row-oriented writer to a file path using the specified column definitions.
        /// Note that any MapToColumn or ParquetDecimalScale attributes will be overridden by the column definitions.
        /// </summary>
        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            string path,
            Column[] columns,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columnsToUse, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columns);
            return new ParquetRowWriter<TTuple>(path, columnsToUse, compression, keyValueMetadata, writeDelegate);
        }

        /// <summary>
        /// Create a row-oriented writer to a file path using the specified writerProperties and column definitions.
        /// Note that any MapToColumn or ParquetDecimalScale attributes will be overridden by the column definitions.
        /// </summary>
        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            string path,
            WriterProperties writerProperties,
            Column[] columns,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columnsToUse, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columns);
            return new ParquetRowWriter<TTuple>(path, columnsToUse, writerProperties, keyValueMetadata, writeDelegate);
        }

        /// <summary>
        /// Create a row-oriented writer to an output stream using the specified column definitions.
        /// Note that any MapToColumn or ParquetDecimalScale attributes will be overridden by the column definitions.
        /// </summary>
        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            OutputStream outputStream,
            Column[] columns,
            Compression compression = Compression.Snappy,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columnsToUse, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columns);
            return new ParquetRowWriter<TTuple>(outputStream, columnsToUse, compression, keyValueMetadata, writeDelegate);
        }

        /// <summary>
        /// Create a row-oriented writer to an output stream using the specified writerProperties and column definitions.
        /// Note that any MapToColumn or ParquetDecimalScale attributes will be overridden by the column definitions.
        /// </summary>
        public static ParquetRowWriter<TTuple> CreateRowWriter<TTuple>(
            OutputStream outputStream,
            WriterProperties writerProperties,
            Column[] columns,
            IReadOnlyDictionary<string, string>? keyValueMetadata = null)
        {
            var (columnsToUse, writeDelegate) = GetOrCreateWriteDelegate<TTuple>(columns);
            return new ParquetRowWriter<TTuple>(outputStream, columnsToUse, writerProperties, keyValueMetadata, writeDelegate);
        }

        private static ParquetRowReader<TTuple>.ReadAction GetOrCreateReadDelegate<TTuple>(MappedField[] fields)
        {
            return (ParquetRowReader<TTuple>.ReadAction) ReadDelegatesCache.GetOrAdd(typeof(TTuple), k => CreateReadDelegate<TTuple>(fields));
        }

        private static (Column[] columns, ParquetRowWriter<TTuple>.WriteAction writeDelegate) GetOrCreateWriteDelegate<TTuple>(string[]? columnNames)
        {
            var (fields, writeDelegate) = WriteDelegates.GetOrAdd(typeof(TTuple), k => CreateWriteDelegate<TTuple>());
            var columns = fields.Select(GetColumn).ToArray();
            if (columnNames != null)
            {
                if (columnNames.Length != columns.Length)
                {
                    throw new ArgumentException(
                        $"The length of column names ({columnNames.Length}) does not mach the number of " +
                        $"public fields and properties ({columns.Length})", nameof(columnNames));
                }

                columns = columns.Select((c, i) => new Column(c.LogicalSystemType, columnNames[i], c.LogicalTypeOverride, c.Length)).ToArray();
            }

            return (columns, (ParquetRowWriter<TTuple>.WriteAction) writeDelegate);
        }

        private static (Column[] columns, ParquetRowWriter<TTuple>.WriteAction writeDelegate) GetOrCreateWriteDelegate<TTuple>(Column[] columns)
        {
            var (fields, writeDelegate) = WriteDelegates.GetOrAdd(typeof(TTuple), k => CreateWriteDelegate<TTuple>());
            if (columns.Length != fields.Length)
            {
                throw new ArgumentException(
                    $"The number of columns specified ({columns.Length}) does not mach the number of public " +
                    $"fields and properties ({fields.Length})", nameof(columns));
            }
            for (var i = 0; i < columns.Length; ++i)
            {
                if (columns[i].LogicalSystemType != fields[i].Type)
                {
                    throw new ArgumentException(
                        $"Expected a system type of '{fields[i].Type}' for column {i} ({columns[i].Name}) " +
                        $"but received '{columns[i].LogicalSystemType}'", nameof(columns));
                }
            }
            return (columns, (ParquetRowWriter<TTuple>.WriteAction) writeDelegate);
        }

        /// <summary>
        /// Returns a delegate to read rows from individual Parquet columns.
        /// </summary>
        private static ParquetRowReader<TTuple>.ReadAction CreateReadDelegate<TTuple>(MappedField[] fields)
        {
            // Parameters
            var reader = Expression.Parameter(typeof(ParquetRowReader<TTuple>), "reader");
            var tuples = Expression.Parameter(typeof(TTuple[]), "tuples");
            var length = Expression.Parameter(typeof(int), "length");

            // Use constructor or the property setters.
            var ctor = typeof(TTuple).GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, fields.Select(f => f.Type).ToArray(), null);
            if (ctor == null && !IsMemberInitializable(typeof(TTuple), fields))
            {
                // Try to get a private constructor if we can't use public constructors.
                // This is necessary if dealing with internal F# types, as all members on internal types are private.
                ctor = typeof(TTuple).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null,
                    fields.Select(f => f.Type).ToArray(), null);
            }

            // Buffers.
            var buffers = fields.Select(f => Expression.Variable(f.Type.MakeArrayType(), $"buffer_{f.Name}")).ToArray();
            var bufferAssigns = fields.Select((f, i) => (Expression) Expression.Assign(buffers[i], Expression.NewArrayBounds(f.Type, length))).ToArray();

            // Read the columns from Parquet and populate the buffers.
            var reads = buffers.Select((buffer, i) => Expression.Call(reader, GetReadMethod<TTuple>(fields[i].Type), Expression.Constant(i), buffer, length)).ToArray();

            // Loop over the tuples, constructing them from the column buffers.
            var index = Expression.Variable(typeof(int), "index");
            var loop = For(index, Expression.Constant(0), Expression.NotEqual(index, length), Expression.PreIncrementAssign(index),
                Expression.Assign(
                    Expression.ArrayAccess(tuples, index),
                    ctor == null
                        ? Expression.MemberInit(Expression.New(typeof(TTuple)), fields.Select((f, i) => Expression.Bind(f.Info, Expression.ArrayAccess(buffers[i], index))))
                        : (Expression) Expression.New(ctor, fields.Select((f, i) => (Expression) Expression.ArrayAccess(buffers[i], index)))
                )
            );

            var body = Expression.Block(buffers, bufferAssigns.Concat(reads).Concat(new[] {loop}));
            var lambda = Expression.Lambda<ParquetRowReader<TTuple>.ReadAction>(body, reader, tuples, length);
            OnReadExpressionCreated?.Invoke(lambda);
            return lambda.Compile();
        }

        /// <summary>
        /// Return a delegate to write rows to individual Parquet columns, as well the fields to be mapped to columns.
        /// </summary>
        private static (MappedField[] fields, ParquetRowWriter<TTuple>.WriteAction writeDelegate) CreateWriteDelegate<TTuple>()
        {
            var fields = GetFieldsAndProperties(typeof(TTuple));

            // Parameters
            var writer = Expression.Parameter(typeof(ParquetRowWriter<TTuple>), "writer");
            var tuples = Expression.Parameter(typeof(TTuple[]), "tuples");
            var length = Expression.Parameter(typeof(int), "length");

            var columnBodies = fields.Select(f =>
            {
                // Column buffer
                var bufferType = f.Type.MakeArrayType();
                var buffer = Expression.Variable(bufferType, $"buffer_{f.Name}");
                var bufferAssign = Expression.Assign(buffer, Expression.NewArrayBounds(f.Type, length));
                var bufferReset = Expression.Assign(buffer, Expression.Constant(null, bufferType));

                // Loop over the tuples and fill the current column buffer.
                var index = Expression.Variable(typeof(int), "index");
                var loop = For(index, Expression.Constant(0), Expression.NotEqual(index, length), Expression.PreIncrementAssign(index),
                    Expression.Assign(
                        Expression.ArrayAccess(buffer, index),
                        Expression.PropertyOrField(Expression.ArrayAccess(tuples, index), f.Name)
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
            return (fields, lambda.Compile());
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

            return Expression.Block(new[] {loopVar},
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

        private static MappedField[] GetFieldsAndProperties(Type type)
        {
            // Members mapped to Parquet columns are any public fields and properties,
            // and private fields and properties annotated with the
            // MapToColumn attribute. Allowing private properties is required for mapping
            // internal types in F#, in which all members are always private.
            var list = new List<MappedField>();
            var flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTuple<,,,,,,,>))
            {
                throw new ArgumentException("System.ValueTuple TTuple types beyond 7 in length are not supported");
            }

            foreach (var field in type.GetFields(flags))
            {
                var mappedColumn = field.GetCustomAttribute<MapToColumnAttribute>()?.ColumnName;
                if (field.IsPublic || mappedColumn != null)
                {
                    list.Add(new MappedField(field.Name, mappedColumn, field.FieldType, field));
                }
            }

            foreach (var property in type.GetProperties(flags))
            {
                var mappedColumn = property.GetCustomAttribute<MapToColumnAttribute>()?.ColumnName;
                if ((property.GetMethod?.IsPublic ?? false) || mappedColumn != null)
                {
                    list.Add(new MappedField(property.Name, mappedColumn, property.PropertyType, property));
                }
            }

            if (list.Count == 0)
            {
                throw new ArgumentException(
                    $"Type '{type}' does not have any public fields or properties to map to Parquet columns, " +
                    $"or any private fields or properties annotated with '{nameof(MapToColumnAttribute)}'", nameof(type));
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
            // are determined at runtime and not by the type. As a resut it is pretty much impossible to cover this with a unit test. Hence this
            // rather long comment aimed at avoiding accidental removal!
            return list.OrderBy(x => x.Info.MetadataToken).ToArray();
        }

        private static Column GetColumn(MappedField field)
        {
            var isDecimal = field.Type == typeof(decimal) || field.Type == typeof(decimal?);
            var decimalScale = field.Info.GetCustomAttributes(typeof(ParquetDecimalScaleAttribute))
                .Cast<ParquetDecimalScaleAttribute>()
                .SingleOrDefault();

            if (!isDecimal && decimalScale != null)
            {
                throw new ArgumentException($"field '{field.Name}' has a {nameof(ParquetDecimalScaleAttribute)} despite not being a decimal type");
            }

            if (isDecimal && decimalScale == null)
            {
                throw new ArgumentException($"field '{field.Name}' has no {nameof(ParquetDecimalScaleAttribute)} despite being a decimal type");
            }

            return new Column(field.Type, field.MappedColumn ?? field.Name, isDecimal ? LogicalType.Decimal(29, decimalScale!.Scale) : null);
        }

        private static bool IsMemberInitializable(Type type, MappedField[] fields)
        {
            if (!type.IsValueType &&
                type.GetConstructor(
                    BindingFlags.Public | BindingFlags.Instance, null, Array.Empty<Type>(), null) ==
                null)
            {
                // No default constructor
                return false;
            }

            foreach (var field in fields)
            {
                var memberType = field.Info.MemberType;
                if (memberType == MemberTypes.Field && !((FieldInfo) field.Info).IsPublic)
                {
                    return false;
                }
                if (memberType == MemberTypes.Property && !(((PropertyInfo) field.Info).SetMethod?.IsPublic ?? false))
                {
                    return false;
                }
            }

            return true;
        }

        private static readonly ConcurrentDictionary<Type, Delegate> ReadDelegatesCache =
            new ConcurrentDictionary<Type, Delegate>();

        private static readonly ConcurrentDictionary<Type, (MappedField[] fields, Delegate writeDelegate)> WriteDelegates =
            new ConcurrentDictionary<Type, (MappedField[] fields, Delegate writeDelegate)>();
    }
}
