using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read;
using SKBKontur.Cassandra.CassandraClient.Commands.Simple.Write;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Write;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

using SkbKontur.Cassandra.TimeBasedUuid;
using SkbKontur.Cassandra.TimeBasedUuid.Bits;

using ApacheConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    internal class ColumnFamilyConnectionImplementation : IColumnFamilyConnectionImplementation
    {
        public ColumnFamilyConnectionImplementation(string keyspaceName,
                                                    string columnFamilyName,
                                                    ICassandraClusterSettings cassandraClusterSettings,
                                                    ICommandExecutor<ISimpleCommand> commandExecutor,
                                                    ICommandExecutor<IFierceCommand> fierceCommandExecutor)
        {
            this.keyspaceName = keyspaceName;
            this.columnFamilyName = columnFamilyName;
            this.cassandraClusterSettings = cassandraClusterSettings;
            this.commandExecutor = commandExecutor;
            this.fierceCommandExecutor = fierceCommandExecutor;
            readConsistencyLevel = cassandraClusterSettings.ReadConsistencyLevel.ToThriftConsistencyLevel();
            writeConsistencyLevel = cassandraClusterSettings.WriteConsistencyLevel.ToThriftConsistencyLevel();
            connectionParameters = new CassandraConnectionParameters(cassandraClusterSettings);
        }

        public bool IsRowExist(byte[] key)
        {
            var keys = GetKeys(key, 1);
            return keys.Count == 1 && ByteArrayComparer.Instance.Equals(keys[0], key);
        }

        public int GetCount(byte[] key)
        {
            var getCountCommand = new GetCountCommand(keyspaceName, columnFamilyName, key, readConsistencyLevel);
            commandExecutor.Execute(getCountCommand);
            return getCountCommand.Count;
        }

        public Dictionary<byte[], int> GetCounts(List<byte[]> key)
        {
            var getCountCommand = new MultiGetCountCommand(keyspaceName, columnFamilyName, readConsistencyLevel, key, null);
            commandExecutor.Execute(getCountCommand);
            return getCountCommand.Output;
        }

        public void DeleteRow(byte[] key, long? timestamp)
        {
            commandExecutor.Execute(new DeleteRowCommand(keyspaceName, columnFamilyName, key, writeConsistencyLevel, timestamp));
        }

        public void AddColumn(byte[] key, RawColumn column)
        {
            var command = CreateInsertCommand(0, attempt => new KeyColumnPair<byte[], RawColumn>(key, column));
            commandExecutor.Execute(command);
        }

        public void AddColumn(Func<int, KeyColumnPair<byte[], RawColumn>> createKeyColumnPair)
        {
            commandExecutor.Execute(attempt => CreateInsertCommand(attempt, createKeyColumnPair));
        }

        public List<KeyValuePair<byte[], List<RawColumn>>> GetRegion(List<byte[]> keys, byte[] startColumnName, byte[] finishColumnName, int limitPerRow)
        {
            var slicePredicate = new SlicePredicate(new SliceRange
                {
                    Count = limitPerRow,
                    StartColumn = startColumnName,
                    EndColumn = finishColumnName,
                    Reversed = false
                });
            var command = new MultiGetSliceCommand(keyspaceName, columnFamilyName, readConsistencyLevel, keys, slicePredicate);
            commandExecutor.Execute(command);
            return command.Output.Where(pair => pair.Value.Any()).ToList();
        }

        public RawColumn GetColumn(byte[] key, byte[] columnName)
        {
            if (!TryGetColumn(key, columnName, out var result))
                throw new ColumnIsNotFoundException(columnFamilyName, key, columnName);
            return result;
        }

        public bool TryGetColumn(byte[] key, byte[] columnName, out RawColumn result)
        {
            result = null;
            var getCommand = new GetCommand(keyspaceName, columnFamilyName, key, readConsistencyLevel, columnName);
            commandExecutor.Execute(getCommand);
            if (getCommand.Output == null)
                return false;
            result = getCommand.Output;
            return true;
        }

        public void AddBatch(byte[] key, List<RawColumn> columns)
        {
            var mutationsList = ToMutationsList(columns, cassandraClusterSettings.AllowNullTimestamp);
            ExecuteMutations(key, mutationsList);
        }

        public void AddBatch(Func<int, KeyColumnsPair<byte[], RawColumn>> createKeyColumnsPair)
        {
            ExecuteMutations(attempt =>
                {
                    var pair = createKeyColumnsPair(attempt);
                    return new KeyValuePair<byte[], List<IMutation>>(
                        pair.Key,
                        ToMutationsList(pair.Columns, cassandraClusterSettings.AllowNullTimestamp));
                });
        }

        public void DeleteBatch(byte[] key, List<byte[]> columnNames, long? timestamp = null)
        {
            var mutationsList = new List<IMutation>
                {
                    new DeletionMutation
                        {
                            SlicePredicate = new SlicePredicate(columnNames),
                            Timestamp = timestamp ?? Timestamp.Now.Ticks
                        }
                };
            ExecuteMutations(key, mutationsList);
        }

        public List<RawColumn> GetRow(byte[] key, byte[] startColumnName, int count, bool reversed)
        {
            return GetRow(key, startColumnName, null, count, reversed);
        }

        public List<RawColumn> GetRow(byte[] key, byte[] startColumnName, byte[] endColumnName, int count, bool reversed)
        {
            var aquilesSlicePredicate = new SlicePredicate(new SliceRange
                {
                    Count = count,
                    StartColumn = startColumnName,
                    EndColumn = endColumnName,
                    Reversed = reversed
                });
            var getSliceCommand = new GetSliceCommand(keyspaceName, columnFamilyName, key, readConsistencyLevel, aquilesSlicePredicate);
            commandExecutor.Execute(getSliceCommand);
            return getSliceCommand.Output;
        }

        public List<RawColumn> GetColumns(byte[] key, List<byte[]> columnNames)
        {
            var slicePredicate = new SlicePredicate(columnNames);
            var getSliceCommand = new GetSliceCommand(keyspaceName, columnFamilyName, key, readConsistencyLevel, slicePredicate);
            commandExecutor.Execute(getSliceCommand);
            return getSliceCommand.Output;
        }

        public List<byte[]> GetKeys(byte[] startKey, int count)
        {
            var keyRange = new KeyRange {StartKey = startKey ?? new byte[0], EndKey = new byte[0], Count = count};
            var aquilesSlicePredicate = new SlicePredicate(new List<byte[]>());
            var getKeyRangeSliceCommand = new GetKeyRangeSliceCommand(keyspaceName, columnFamilyName, readConsistencyLevel, keyRange, aquilesSlicePredicate);

            commandExecutor.Execute(getKeyRangeSliceCommand);
            return getKeyRangeSliceCommand.Output;
        }

        public ICassandraConnectionParameters GetConnectionParameters()
        {
            return connectionParameters;
        }

        public List<KeyValuePair<byte[], List<RawColumn>>> GetRows(List<byte[]> keys, byte[] startColumnName, int count)
        {
            var multiGetSliceCommand = new MultiGetSliceCommand(keyspaceName, columnFamilyName, readConsistencyLevel,
                                                                keys,
                                                                new SlicePredicate(new SliceRange
                                                                    {
                                                                        Count = count,
                                                                        StartColumn = startColumnName
                                                                    }));
            commandExecutor.Execute(multiGetSliceCommand);
            return multiGetSliceCommand.Output.Where(pair => pair.Value.Any()).ToList();
        }

        public List<KeyValuePair<byte[], List<RawColumn>>> GetRows(List<byte[]> keys, List<byte[]> columnNames)
        {
            var multiGetSliceCommand = new MultiGetSliceCommand(keyspaceName, columnFamilyName, readConsistencyLevel,
                                                                keys,
                                                                new SlicePredicate(columnNames));
            commandExecutor.Execute(multiGetSliceCommand);
            return multiGetSliceCommand.Output.Where(pair => pair.Value.Any()).ToList();
        }

        public void Truncate()
        {
            var truncateCommand = new TruncateColumnFamilyCommand(keyspaceName, columnFamilyName);
            fierceCommandExecutor.Execute(truncateCommand);
        }

        public void BatchInsert(List<KeyValuePair<byte[], List<RawColumn>>> data)
        {
            var mutationsList = data.Select(row => new KeyValuePair<byte[], List<IMutation>>(row.Key, ToMutationsList(row.Value, cassandraClusterSettings.AllowNullTimestamp))).ToList();
            ExecuteMutations(mutationsList);
        }

        public void BatchDelete(List<KeyValuePair<byte[], List<byte[]>>> data, long? timestamp = null)
        {
            var mutationsList = data.Select(
                row => new KeyValuePair<byte[], List<IMutation>>(row.Key,
                                                                 new List<IMutation>
                                                                     {
                                                                         new DeletionMutation
                                                                             {
                                                                                 SlicePredicate = new SlicePredicate(row.Value),
                                                                                 Timestamp = timestamp ?? Timestamp.Now.Ticks
                                                                             }
                                                                     })).ToList();
            ExecuteMutations(mutationsList);
        }

        private ISimpleCommand CreateInsertCommand(int attempt, Func<int, KeyColumnPair<byte[], RawColumn>> createKeyColumnPair)
        {
            var keyColumnPair = createKeyColumnPair(attempt);
            CheckColumnHasTimestampValue(keyColumnPair.Column);
            return new InsertCommand(keyspaceName, columnFamilyName, keyColumnPair.Key, writeConsistencyLevel, keyColumnPair.Column);
        }

        [SuppressMessage("ReSharper", "ParameterOnlyUsedForPreconditionCheck.Local")]
        private void CheckColumnHasTimestampValue(RawColumn column)
        {
            if (!cassandraClusterSettings.AllowNullTimestamp && !column.Timestamp.HasValue)
                throw new ArgumentException("Timestamp should be filled.");
        }

        private static List<IMutation> ToMutationsList(List<RawColumn> columns, bool allowNullTimestamp)
        {
            var result = new List<IMutation>();
            foreach (var column in columns)
            {
                if (!allowNullTimestamp && !column.Timestamp.HasValue)
                    throw new ArgumentException("Timestamp should be filled.");
                result.Add(new SetMutation
                    {
                        Column = column
                    });
            }
            return result;
        }

        private void ExecuteMutations(byte[] key, List<IMutation> mutationsList)
        {
            var columnFamilyMutations = new Dictionary<byte[], List<IMutation>>
                {
                    {key, mutationsList}
                };

            var keyMutations = new Dictionary<string, Dictionary<byte[], List<IMutation>>>
                {
                    {columnFamilyName, columnFamilyMutations}
                };

            var batchMutateCommand = new BatchMutateCommand(keyspaceName, columnFamilyName, writeConsistencyLevel, keyMutations);

            commandExecutor.Execute(batchMutateCommand);
        }

        private void ExecuteMutations(Func<int, KeyValuePair<byte[], List<IMutation>>> createKeyMutationsListPair)
        {
            commandExecutor.Execute(attempt =>
                {
                    var keyMutationsListPair = createKeyMutationsListPair(attempt);

                    var columnFamilyMutations = new Dictionary<byte[], List<IMutation>>
                        {
                            {keyMutationsListPair.Key, keyMutationsListPair.Value}
                        };

                    var keyMutations = new Dictionary<string, Dictionary<byte[], List<IMutation>>>
                        {
                            {columnFamilyName, columnFamilyMutations}
                        };

                    return new BatchMutateCommand(keyspaceName, columnFamilyName, writeConsistencyLevel, keyMutations);
                });
        }

        private void ExecuteMutations(IEnumerable<KeyValuePair<byte[], List<IMutation>>> mutationsList)
        {
            var dict = mutationsList.ToDictionary(item => item.Key, item => item.Value);
            var keyMutations = new Dictionary<string, Dictionary<byte[], List<IMutation>>>
                {
                    {columnFamilyName, dict}
                };

            var batchMutateCommand = new BatchMutateCommand(keyspaceName, columnFamilyName, writeConsistencyLevel, keyMutations);

            commandExecutor.Execute(batchMutateCommand);
        }

        private readonly string keyspaceName;
        private readonly string columnFamilyName;
        private readonly ICassandraClusterSettings cassandraClusterSettings;
        private readonly ICommandExecutor<ISimpleCommand> commandExecutor;
        private readonly ICommandExecutor<IFierceCommand> fierceCommandExecutor;
        private readonly ICassandraConnectionParameters connectionParameters;
        private readonly ApacheConsistencyLevel readConsistencyLevel;
        private readonly ApacheConsistencyLevel writeConsistencyLevel;
    }
}