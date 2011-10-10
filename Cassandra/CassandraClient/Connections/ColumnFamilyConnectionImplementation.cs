using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Helpers;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

using AquilesConsistencyLevel = SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.AquilesConsistencyLevel;
using BatchMutateCommand = SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.BatchMutateCommand;
using Column = SKBKontur.Cassandra.CassandraClient.Abstractions.Column;
using ConsistencyLevel = SKBKontur.Cassandra.CassandraClient.Abstractions.ConsistencyLevel;
using GetCommand = SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.GetCommand;
using GetIndexedSlicesCommand = SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.GetIndexedSlicesCommand;
using GetKeyRangeSliceCommand = SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.GetKeyRangeSliceCommand;
using GetSliceCommand = SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.GetSliceCommand;
using InsertCommand = SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.InsertCommand;
using MultiGetSliceCommand = SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.MultiGetSliceCommand;
using TruncateColumnFamilyCommand = SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.TruncateColumnFamilyCommand;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public class ColumnFamilyConnectionImplementation : IColumnFamilyConnectionImplementation
    {
        public ColumnFamilyConnectionImplementation(string keyspaceName,
                                                    string columnFamilyName,
                                                    ICommandExecuter commandExecuter,
                                                    Abstractions.ConsistencyLevel readConsistencyLevel,
                                                    Abstractions.ConsistencyLevel writeConsistencyLevel)
        {
            this.keyspaceName = keyspaceName;
            this.columnFamilyName = columnFamilyName;
            this.commandExecuter = commandExecuter;
            this.readConsistencyLevel = readConsistencyLevel.ToAquilesConsistencyLevel();
            this.writeConsistencyLevel = writeConsistencyLevel.ToAquilesConsistencyLevel();
        }

        public void Dispose()
        {
            //aquilesConnection.Dispose();
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

        public void AddColumn(byte[] key, Abstractions.Column column)
        {
            ExecuteCommand(new AquilesTrash.Command.InsertCommand
                {
                    Column = column.ToAquilesColumn(),
                    Key = key,
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = writeConsistencyLevel
                });
        }

        public Abstractions.Column GetColumn(byte[] key, byte[] columnName)
        {
            Abstractions.Column result;
            if(!TryGetColumn(key, columnName, out result))
                throw new ColumnIsNotFoundException(columnFamilyName, key, columnName);
            return result;
        }

        public bool TryGetColumn(byte[] key, byte[] columnName, out Abstractions.Column result)
        {
            result = null;
            var getCommand = new AquilesTrash.Command.GetCommand
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

        public void AddBatch(byte[] key, IEnumerable<Abstractions.Column> columns)
        {
            List<IAquilesMutation> mutationsList = ToMutationsList(columns);
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

        public Abstractions.Column[] GetRow(byte[] key, byte[] startColumnName, int count)
        {
            var getSliceCommand = new AquilesTrash.Command.GetSliceCommand
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
            var getKeyRangeSliceCommand = new AquilesTrash.Command.GetKeyRangeSliceCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = readConsistencyLevel,
                    Predicate = new AquilesSlicePredicate {Columns = new List<byte[]>()},
                    KeyTokenRange = new AquilesKeyRange {StartKey = startKey ?? new byte[0], EndKey=new byte[0], Count = count}
                };
            ExecuteCommand(getKeyRangeSliceCommand);
            return getKeyRangeSliceCommand.Output;
        }

        public List<KeyValuePair<byte[], Abstractions.Column[]>> GetRows(IEnumerable<byte[]> keys, byte[] startColumnName, int count)
        {
            var multiGetSliceCommand = new AquilesTrash.Command.MultiGetSliceCommand
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
            return multiGetSliceCommand.Output.Select(item => new KeyValuePair<byte[], Abstractions.Column[]>(item.Key, item.Value.Select(@out => @out.ToColumn()).ToArray())).Where(pair => pair.Value.Length > 0).ToList();
        }

        public void Truncate()
        {
            var truncateCommand = new AquilesTrash.Command.TruncateColumnFamilyCommand
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

            var gisc = new AquilesTrash.Command.GetIndexedSlicesCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = readConsistencyLevel,
                    Predicate = predicate,
                    IndexClause = indexClause
                };

            ExecuteCommand(gisc);
            return gisc.Output;
        }

        public void BatchInsert(IEnumerable<KeyValuePair<byte[], IEnumerable<Abstractions.Column>>> data)
        {
            List<KeyValuePair<byte[], List<IAquilesMutation>>> mutationsList = data.Select(row => new KeyValuePair<byte[], List<IAquilesMutation>>(row.Key, ToMutationsList(row.Value))).ToList();
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

        

        private static List<IAquilesMutation> ToMutationsList(IEnumerable<Abstractions.Column> columns)
        {
            return columns.Select(column => new AquilesSetMutation {Column = column.ToAquilesColumn()}).Cast<IAquilesMutation>().ToList();
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

            var batchMutateCommand = new AquilesTrash.Command.BatchMutateCommand
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

            var batchMutateCommand = new AquilesTrash.Command.BatchMutateCommand
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
        private readonly AquilesTrash.Command.AquilesConsistencyLevel readConsistencyLevel;
        private readonly AquilesTrash.Command.AquilesConsistencyLevel writeConsistencyLevel;
    }
}