using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;
using SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read;
using SKBKontur.Cassandra.CassandraClient.Commands.Simple.Write;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Write;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

using ApacheConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public class ColumnFamilyConnectionImplementation : IColumnFamilyConnectionImplementation
    {
        public ColumnFamilyConnectionImplementation(string keyspaceName,
                                                    string columnFamilyName,
                                                    ICassandraClusterSettings cassandraClusterSettings,
                                                    ICommandExecuter commandExecuter,
                                                    ConsistencyLevel readConsistencyLevel,
                                                    ConsistencyLevel writeConsistencyLevel)
        {
            this.keyspaceName = keyspaceName;
            this.columnFamilyName = columnFamilyName;
            this.cassandraClusterSettings = cassandraClusterSettings;
            this.commandExecuter = commandExecuter;
            this.readConsistencyLevel = readConsistencyLevel.ToThriftConsistencyLevel();
            this.writeConsistencyLevel = writeConsistencyLevel.ToThriftConsistencyLevel();
        }

        public bool IsRowExist(byte[] key)
        {
            List<byte[]> keys = GetKeys(key, 1);
            return keys.Count == 1 && ByteArrayEqualityComparer.SimpleComparer.Equals(keys[0], key);
        }

        public int GetCount(byte[] key)
        {
            var getCountCommand = new GetCountCommand(keyspaceName, columnFamilyName, key, readConsistencyLevel);
            ExecuteCommand(getCountCommand);
            return getCountCommand.Count;
        }

        public Dictionary<byte[], int> GetCounts(IEnumerable<byte[]> key)
        {
            var getCountCommand = new MultiGetCountCommand(keyspaceName, columnFamilyName, readConsistencyLevel, key.ToList(), null);
            ExecuteCommand(getCountCommand);
            return getCountCommand.Output;
        }

        public void DeleteRow(byte[] key, long? timestamp)
        {
            ExecuteCommand(new DeleteRowCommand(keyspaceName, columnFamilyName, key, writeConsistencyLevel, timestamp));
        }

        public void AddColumn(byte[] key, Column column)
        {
            KeyspaceColumnFamilyDependantCommandBase command = CreateInsertCommand(0, attempt => new KeyColumnPair<byte[]>(key, column));
            ExecuteCommand(command);
        }

        public void AddColumn(Func<int, KeyColumnPair<byte[]>> createKeyColumnPair)
        {
            ExecuteCommand(attempt => CreateInsertCommand(attempt, createKeyColumnPair));
        }

        public Column GetColumn(byte[] key, byte[] columnName)
        {
            Column result;
            if(!TryGetColumn(key, columnName, out result))
                throw new ColumnIsNotFoundException(columnFamilyName, key, columnName);
            return result;
        }

        public bool TryGetColumn(byte[] key, byte[] columnName, out Column result)
        {
            result = null;
            var getCommand = new GetCommand(keyspaceName, columnFamilyName, key, readConsistencyLevel, columnName);
            ExecuteCommand(getCommand);
            if(getCommand.Output == null || getCommand.Output == null)
                return false;
            result = getCommand.Output;
            return true;
        }

        public void AddBatch(byte[] key, IEnumerable<Column> columns)
        {
            List<IMutation> mutationsList = ToMutationsList(columns, cassandraClusterSettings.AllowNullTimestamp);
            ExecuteMutations(key, mutationsList);
        }

        public void AddBatch(Func<int, KeyColumnsPair<byte[]>> createKeyColumnsPair)
        {
            ExecuteMutations(attempt =>
                {
                    KeyColumnsPair<byte[]> pair = createKeyColumnsPair(attempt);
                    return new KeyValuePair<byte[], List<IMutation>>(
                        pair.Key,
                        ToMutationsList(pair.Columns, cassandraClusterSettings.AllowNullTimestamp));
                });
        }

        public void DeleteBatch(byte[] key, IEnumerable<byte[]> columnNames, long? timestamp = null)
        {
            var mutationsList = new List<IMutation>
                {
                    new DeletionMutation
                        {
                            SlicePredicate = new SlicePredicate(columnNames.ToList()),
                            Timestamp = timestamp ?? DateTimeService.UtcNow.Ticks
                        }
                };
            ExecuteMutations(key, mutationsList);
        }

        public Column[] GetRow(byte[] key, byte[] startColumnName, int count, bool reversed)
        {
            return GetRow(key, startColumnName, null, count, reversed);
        }

        public Column[] GetRow(byte[] key, byte[] startColumnName, byte[] endColumnName, int count, bool reversed)
        {
            var aquilesSlicePredicate = new SlicePredicate(new SliceRange
                {
                    Count = count,
                    StartColumn = startColumnName,
                    EndColumn = endColumnName,
                    Reversed = reversed
                });
            var getSliceCommand = new GetSliceCommand(keyspaceName, columnFamilyName, key, readConsistencyLevel, aquilesSlicePredicate);
            ExecuteCommand(getSliceCommand);
            return getSliceCommand.Output.ToArray();
        }

        public Column[] GetColumns(byte[] key, List<byte[]> columnNames)
        {
            var slicePredicate = new SlicePredicate(columnNames);
            var getSliceCommand = new GetSliceCommand(keyspaceName, columnFamilyName, key, readConsistencyLevel, slicePredicate);
            ExecuteCommand(getSliceCommand);
            return getSliceCommand.Output.ToArray();
        }

        public List<byte[]> GetKeys(byte[] startKey, int count)
        {
            var keyRange = new KeyRange {StartKey = startKey ?? new byte[0], EndKey = new byte[0], Count = count};
            var aquilesSlicePredicate = new SlicePredicate(new List<byte[]>());
            var getKeyRangeSliceCommand = new GetKeyRangeSliceCommand(keyspaceName, columnFamilyName, readConsistencyLevel, keyRange, aquilesSlicePredicate);

            ExecuteCommand(getKeyRangeSliceCommand);
            return getKeyRangeSliceCommand.Output;
        }

        public List<KeyValuePair<byte[], Column[]>> GetRows(IEnumerable<byte[]> keys, byte[] startColumnName, int count)
        {
            var multiGetSliceCommand = new MultiGetSliceCommand(keyspaceName, columnFamilyName, readConsistencyLevel,
                                                                keys.ToList(),
                                                                new SlicePredicate(new SliceRange
                                                                    {
                                                                        Count = count,
                                                                        StartColumn = startColumnName
                                                                    }));
            ExecuteCommand(multiGetSliceCommand);
            return multiGetSliceCommand.Output.Select(item => new KeyValuePair<byte[], Column[]>(item.Key, item.Value.ToArray())).Where(pair => pair.Value.Length > 0).ToList();
        }

        public void Truncate()
        {
            var truncateCommand = new TruncateColumnFamilyCommand(keyspaceName, columnFamilyName);
            ExecuteCommand(truncateCommand);
        }

        public List<byte[]> GetRowsWhere(byte[] startKey, int maximalCount, IndexExpression[] conditions, List<byte[]> columns)
        {
            var predicate = new SlicePredicate(columns);
            var indexClause = new IndexClause
                {
                    Count = maximalCount,
                    Expressions = (conditions ?? new IndexExpression[0]).ToList(),
                    StartKey = startKey ?? new byte[0]
                };
            var gisc = new GetIndexedSlicesCommand(keyspaceName, columnFamilyName, readConsistencyLevel, predicate, indexClause);

            ExecuteCommand(gisc);
            return gisc.Output;
        }

        public void BatchInsert(IEnumerable<KeyValuePair<byte[], IEnumerable<Column>>> data)
        {
            List<KeyValuePair<byte[], List<IMutation>>> mutationsList = data.Select(row => new KeyValuePair<byte[], List<IMutation>>(row.Key, ToMutationsList(row.Value, cassandraClusterSettings.AllowNullTimestamp))).ToList();
            ExecuteMutations(mutationsList);
        }

        public void BatchDelete(IEnumerable<KeyValuePair<byte[], IEnumerable<byte[]>>> data, long? timestamp = null)
        {
            List<KeyValuePair<byte[], List<IMutation>>> mutationsList = data.Select(
                row => new KeyValuePair<byte[], List<IMutation>>(row.Key,
                                                                 new List<IMutation>
                                                                     {
                                                                         new DeletionMutation
                                                                             {
                                                                                 SlicePredicate = new SlicePredicate(row.Value.ToList()),
                                                                                 Timestamp = timestamp ?? DateTimeService.UtcNow.Ticks
                                                                             }
                                                                     })).ToList();
            ExecuteMutations(mutationsList);
        }

        private KeyspaceColumnFamilyDependantCommandBase CreateInsertCommand(int attempt, Func<int, KeyColumnPair<byte[]>> createKeyColumnPair)
        {
            KeyColumnPair<byte[]> keyColumnPair = createKeyColumnPair(attempt);
            CheckColumnHasTimestampValue(keyColumnPair.Column);
            return new InsertCommand(keyspaceName, columnFamilyName, keyColumnPair.Key, writeConsistencyLevel, keyColumnPair.Column);
        }

        private void CheckColumnHasTimestampValue(Column column)
        {
            if(!cassandraClusterSettings.AllowNullTimestamp && !column.Timestamp.HasValue)
                throw new ArgumentException(string.Format("Timestamp should be filled. Column: '{0}'", column.Name));
        }

        private static List<IMutation> ToMutationsList(IEnumerable<Column> columns, bool allowNullTimestamp)
        {
            var result = new List<IMutation>();
            foreach(Column column in columns)
            {
                if(!allowNullTimestamp && !column.Timestamp.HasValue)
                    throw new ArgumentException(string.Format("Timestamp should be filled. Column: '{0}'", column.Name));
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

            ExecuteCommand(batchMutateCommand);
        }

        private void ExecuteMutations(Func<int, KeyValuePair<byte[], List<IMutation>>> createKeyMutationsListPair)
        {
            ExecuteCommand(attempt =>
                {
                    KeyValuePair<byte[], List<IMutation>> keyMutationsListPair = createKeyMutationsListPair(attempt);

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
            Dictionary<byte[], List<IMutation>> dict = mutationsList.ToDictionary(item => item.Key, item => item.Value);
            var keyMutations = new Dictionary<string, Dictionary<byte[], List<IMutation>>>
                {
                    {columnFamilyName, dict}
                };

            var batchMutateCommand = new BatchMutateCommand(keyspaceName, columnFamilyName, writeConsistencyLevel, keyMutations);

            ExecuteCommand(batchMutateCommand);
        }

        private void ExecuteCommand(KeyspaceColumnFamilyDependantCommandBase commandBase)
        {
            commandExecuter.Execute(commandBase);
        }

        private void ExecuteCommand(Func<int, KeyspaceColumnFamilyDependantCommandBase> createCommand)
        {
            commandExecuter.Execute(createCommand);
        }

        private readonly ICommandExecuter commandExecuter;
        private readonly string keyspaceName;
        private readonly string columnFamilyName;
        private readonly ICassandraClusterSettings cassandraClusterSettings;
        private readonly ApacheConsistencyLevel readConsistencyLevel;
        private readonly ApacheConsistencyLevel writeConsistencyLevel;
    }
}