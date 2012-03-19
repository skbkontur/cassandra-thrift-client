using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

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
            this.readConsistencyLevel = readConsistencyLevel.ToAquilesConsistencyLevel();
            this.writeConsistencyLevel = writeConsistencyLevel.ToAquilesConsistencyLevel();
        }

        public bool IsRowExist(byte[] key)
        {
            var keys = GetKeys(key, 1);
            return keys.Count == 1 && ByteArrayEqualityComparer.SimpleComparer.Equals(keys[0], key);
        }

        public int GetCount(byte[] key)
        {
            var getCountCommand = new GetCountCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = readConsistencyLevel,
                    Key = key
                };
            ExecuteCommand(getCountCommand);
            return getCountCommand.Count;
        }

        public Dictionary<byte[], int> GetCounts(IEnumerable<byte[]> key)
        {
            var getCountCommand = new MultiGetCountCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = readConsistencyLevel,
                    Keys = key.ToList()
                };
            ExecuteCommand(getCountCommand);
            return getCountCommand.Output;
        }

        public void DeleteRow(byte[] key, long? timestamp)
        {
            ExecuteCommand(new DeleteRowCommand
                {
                    Key = key,
                    Timestamp = timestamp,
                    ColumnFamily = columnFamilyName
                });
        }

        public void AddColumn(byte[] key, Column column)
        {
            ExecuteCommand(new InsertCommand
                {
                    Column = column.ToAquilesColumn(cassandraClusterSettings.AllowNullTimestamp),
                    Key = key,
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = writeConsistencyLevel
                });
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
            var getCommand = new GetCommand
                {
                    ColumnFamily = columnFamilyName,
                    ColumnName = columnName,
                    ConsistencyLevel = readConsistencyLevel,
                    Key = key
                };
            ExecuteCommand(getCommand);
            if(getCommand.Output == null || getCommand.Output == null)
                return false;
            result = getCommand.Output.ToColumn();
            return true;
        }

        public void AddBatch(byte[] key, IEnumerable<Column> columns)
        {
            List<IAquilesMutation> mutationsList = ToMutationsList(columns, cassandraClusterSettings.AllowNullTimestamp);
            ExecuteMutations(key, mutationsList);
        }

        public void DeleteBatch(byte[] key, IEnumerable<byte[]> columnNames, long? timestamp = null)
        {
            var mutationsList = new List<IAquilesMutation>
                {
                    new AquilesDeletionMutation
                        {
                            Predicate = new AquilesSlicePredicate
                                {
                                    Columns = columnNames.ToList(),
                                },
                            Timestamp = timestamp ?? DateTimeService.UtcNow.Ticks
                        }
                };
            ExecuteMutations(key, mutationsList);
        }

        public Column[] GetRow(byte[] key, byte[] startColumnName, int count)
        {
            var getSliceCommand = new GetSliceCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = readConsistencyLevel,
                    Key = key,
                    Predicate = new AquilesSlicePredicate
                        {
                            SliceRange = new AquilesSliceRange
                                {
                                    Count = count,
                                    StartColumn = startColumnName
                                }
                        }
                };
            ExecuteCommand(getSliceCommand);
            return getSliceCommand.Output.Select(@out => @out.ToColumn()).ToArray();
        }

        public List<byte[]> GetKeys(byte[] startKey, int count)
        {
            var getKeyRangeSliceCommand = new GetKeyRangeSliceCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = readConsistencyLevel,
                    Predicate = new AquilesSlicePredicate {Columns = new List<byte[]>()},
                    KeyTokenRange = new AquilesKeyRange {StartKey = startKey ?? new byte[0], EndKey = new byte[0], Count = count}
                };
            ExecuteCommand(getKeyRangeSliceCommand);
            return getKeyRangeSliceCommand.Output;
        }

        public List<KeyValuePair<byte[], Column[]>> GetRows(IEnumerable<byte[]> keys, byte[] startColumnName, int count)
        {
            var multiGetSliceCommand = new MultiGetSliceCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = readConsistencyLevel,
                    Keys = keys.ToList(),
                    Predicate = new AquilesSlicePredicate
                        {
                            SliceRange = new AquilesSliceRange
                                {
                                    Count = count,
                                    StartColumn = startColumnName
                                }
                        }
                };
            ExecuteCommand(multiGetSliceCommand);
            return multiGetSliceCommand.Output.Select(item => new KeyValuePair<byte[], Column[]>(item.Key, item.Value.Select(@out => @out.ToColumn()).ToArray())).Where(pair => pair.Value.Length > 0).ToList();
        }

        public void Truncate()
        {
            var truncateCommand = new TruncateColumnFamilyCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = writeConsistencyLevel
                };
            ExecuteCommand(truncateCommand);
        }

        public List<byte[]> GetRowsWhere(byte[] startKey, int maximalCount, AquilesIndexExpression[] conditions, List<byte[]> columns)
        {
            var predicate = new AquilesSlicePredicate {Columns = columns};

            var indexClause = new AquilesIndexClause();
            indexClause.Count = maximalCount;
            indexClause.StartKey = startKey ?? new byte[0];
            indexClause.Expressions = new List<AquilesIndexExpression>();
            indexClause.Expressions.AddRange(conditions);

            var gisc = new GetIndexedSlicesCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = readConsistencyLevel,
                    Predicate = predicate,
                    IndexClause = indexClause
                };

            ExecuteCommand(gisc);
            return gisc.Output;
        }

        public void BatchInsert(IEnumerable<KeyValuePair<byte[], IEnumerable<Column>>> data)
        {
            List<KeyValuePair<byte[], List<IAquilesMutation>>> mutationsList = data.Select(row => new KeyValuePair<byte[], List<IAquilesMutation>>(row.Key, ToMutationsList(row.Value, cassandraClusterSettings.AllowNullTimestamp))).ToList();
            ExecuteMutations(mutationsList);
        }

        public void BatchDelete(IEnumerable<KeyValuePair<byte[], IEnumerable<byte[]>>> data, long? timestamp = null)
        {
            List<KeyValuePair<byte[], List<IAquilesMutation>>> mutationsList = data.Select(
                row => new KeyValuePair<byte[], List<IAquilesMutation>>(row.Key,
                                                                        new List<IAquilesMutation>
                                                                            {
                                                                                new AquilesDeletionMutation
                                                                                    {
                                                                                        Predicate = new AquilesSlicePredicate
                                                                                            {
                                                                                                Columns = row.Value.ToList()
                                                                                            },
                                                                                        Timestamp = timestamp ?? DateTimeService.UtcNow.Ticks
                                                                                    }
                                                                            })).ToList();
            ExecuteMutations(mutationsList);
        }

        private static List<IAquilesMutation> ToMutationsList(IEnumerable<Column> columns, bool allowNullTimestamp)
        {
            return columns.Select(column => new AquilesSetMutation { Column = column.ToAquilesColumn(allowNullTimestamp) }).Cast<IAquilesMutation>().ToList();
        }

        private void ExecuteMutations(byte[] key, List<IAquilesMutation> mutationsList)
        {
            var columnFamilyMutations = new Dictionary<byte[], List<IAquilesMutation>>
                {
                    {key, mutationsList}
                };

            var keyMutations = new Dictionary<string, Dictionary<byte[], List<IAquilesMutation>>>
                {
                    {columnFamilyName, columnFamilyMutations}
                };

            var batchMutateCommand = new BatchMutateCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    Mutations = keyMutations
                };

            ExecuteCommand(batchMutateCommand);
        }

        private void ExecuteMutations(IEnumerable<KeyValuePair<byte[], List<IAquilesMutation>>> mutationsList)
        {
            var dict = mutationsList.ToDictionary(item => item.Key, item => item.Value);
            var keyMutations = new Dictionary<string, Dictionary<byte[], List<IAquilesMutation>>>
                {
                    {columnFamilyName, dict}
                };

            var batchMutateCommand = new BatchMutateCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    Mutations = keyMutations
                };

            ExecuteCommand(batchMutateCommand);
        }

        private void ExecuteCommand(IAquilesCommand command)
        {
            commandExecuter.Execute(new AquilesCommandAdaptor(command, keyspaceName));
        }

        private readonly ICommandExecuter commandExecuter;
        private readonly string keyspaceName;
        private readonly string columnFamilyName;
        private readonly ICassandraClusterSettings cassandraClusterSettings;
        private readonly AquilesConsistencyLevel readConsistencyLevel;
        private readonly AquilesConsistencyLevel writeConsistencyLevel;
    }
}