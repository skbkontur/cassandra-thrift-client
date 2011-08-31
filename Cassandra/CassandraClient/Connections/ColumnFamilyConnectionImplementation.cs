using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Apache.Cassandra;

using Aquiles;
using Aquiles.Command;
using Aquiles.Exceptions;
using Aquiles.Model;

using CassandraClient.Abstractions;
using CassandraClient.Core;
using CassandraClient.Exceptions;
using CassandraClient.Helpers;

using Thrift;
using Thrift.Transport;

using Column = CassandraClient.Abstractions.Column;
using ConsistencyLevel = CassandraClient.Abstractions.ConsistencyLevel;

namespace CassandraClient.Connections
{
    public class ColumnFamilyConnectionImplementation : IColumnFamilyConnectionImplementation
    {
        public ColumnFamilyConnectionImplementation(string keyspaceName,
                                                    string columnFamilyName,
                                                    ICommandExecuter commandExecuter,
                                                    ConsistencyLevel readConsistencyLevel,
                                                    ConsistencyLevel writeConsistencyLevel)
        {
            this.keyspaceName = keyspaceName;
            this.columnFamilyName = columnFamilyName;
            this.commandExecuter = commandExecuter;
            this.readConsistencyLevel = readConsistencyLevel.ToAquilesConsistencyLevel();
            this.writeConsistencyLevel = writeConsistencyLevel.ToAquilesConsistencyLevel();
        }

        #region IColumnFamilyConnectionImplementation Members

        public void Dispose()
        {
            //aquilesConnection.Dispose();
        }

        public void AddColumn(byte[] key, Column column)
        {
            ExecuteCommand(new InsertCommand
                {
                    Column = column.ToAquilesColumn(),
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
            if(getCommand.Output == null || getCommand.Output.Column == null)
                return false;
            result = getCommand.Output.Column.ToColumn();
            return true;
        }

        public void AddBatch(byte[] key, IEnumerable<Column> columns)
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
                            Timestamp = timestamp
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
            return getSliceCommand.Output.Results.Select(@out => @out.Column.ToColumn()).ToArray();
        }

        public List<byte[]> GetKeys(byte[] startKey, int count)
        {
            var getKeyRangeSliceCommand = new GetKeyRangeSliceCommand
                {
                    ColumnFamily = columnFamilyName,
                    ConsistencyLevel = readConsistencyLevel,
                    Predicate = new AquilesSlicePredicate {Columns = new List<byte[]>()},
                    KeyTokenRange = new AquilesKeyRange {StartKey = startKey ?? new byte[0], EndKey=new byte[0], Count = count}
                };
            ExecuteCommand(getKeyRangeSliceCommand);
            return getKeyRangeSliceCommand.Output.Select(row => row.Key).ToList();
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
            return multiGetSliceCommand.Output.Results.Select(item => new KeyValuePair<byte[], Column[]>(item.Key, item.Value.Select(@out => @out.Column.ToColumn()).ToArray())).Where(pair => pair.Value.Length > 0).ToList();
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
            return gisc.Output.Select(res => res.Key).ToList();
        }

        public void BatchInsert(IEnumerable<KeyValuePair<byte[], IEnumerable<Column>>> data)
        {
            List<KeyValuePair<byte[], List<IAquilesMutation>>> mutationsList = data.Select(row => new KeyValuePair<byte[], List<IAquilesMutation>>(row.Key, ToMutationsList(row.Value))).ToList();
            ExecuteMutations(mutationsList);
        }

        public void BatchDelete(IEnumerable<KeyValuePair<byte[], IEnumerable<byte[]>>> data)
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
                                                                                            }
                                                                                    }
                                                                            })).ToList();
            ExecuteMutations(mutationsList);
        }

        #endregion

        private static List<IAquilesMutation> ToMutationsList(IEnumerable<Column> columns)
        {
            return columns.Select(column => new AquilesSetMutation {Column = column.ToAquilesColumn()}).Cast<IAquilesMutation>().ToList();
        }

        private void ExecuteMutations(byte[] key, List<IAquilesMutation> mutationsList)
        {
            var columnFamilyMutations = new Dictionary<string, List<IAquilesMutation>>
                {
                    {columnFamilyName, mutationsList}
                };

            var keyMutations = new Dictionary<byte[], Dictionary<string, List<IAquilesMutation>>>
                {
                    {key, columnFamilyMutations}
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
            var keyMutations = mutationsList.ToDictionary(item => item.Key, item => new Dictionary<string, List<IAquilesMutation>> {{columnFamilyName, item.Value}});

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
        private readonly AquilesConsistencyLevel readConsistencyLevel;
        private readonly AquilesConsistencyLevel writeConsistencyLevel;
    }
}