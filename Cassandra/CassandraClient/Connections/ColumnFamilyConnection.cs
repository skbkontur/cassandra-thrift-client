using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public class ColumnFamilyConnection : IColumnFamilyConnection
    {
        public ColumnFamilyConnection(IColumnFamilyConnectionImplementation implementation, IEnumerableFactory enumerableFactory)
        {
            this.implementation = implementation;
            this.enumerableFactory = enumerableFactory;
        }

        public bool IsRowExist(string key)
        {
            return implementation.IsRowExist(StringHelpers.StringToBytes(key));
        }

        public void DeleteRows(string[] keys, long? timestamp = null, int batchSize = 1000)
        {
            var counts = GetCounts(keys);
            while(counts.Count > 0)
            {
                var rows = GetRows(keys, null, batchSize);
                var d = rows.Select(row => new KeyValuePair<string, IEnumerable<string>>(row.Key, row.Value.Select(col => col.Name)));
                BatchDelete(d, timestamp);
                var newCounts = new Dictionary<string, int>();
                foreach(var keyValuePair in counts)
                {
                    if(keyValuePair.Value > batchSize)
                        newCounts.Add(keyValuePair.Key, keyValuePair.Value - batchSize);
                }
                counts = newCounts;
            }
        }

        public void DeleteRow(string key, long? timestamp = null)
        {
            implementation.DeleteRow(StringHelpers.StringToBytes(key), timestamp);
        }

        public void DeleteColumn(string key, string columnName, long? timestamp = null)
        {
            implementation.DeleteBatch(StringHelpers.StringToBytes(key), new[] { StringHelpers.StringToBytes(columnName) }, timestamp);
        }

        public void AddColumn(string key, Column column)
        {
            implementation.AddColumn(StringHelpers.StringToBytes(key), column);
        }

        public void AddColumn(Func<int, KeyColumnPair<string>> createKeyColumnPair)
        {
            implementation.AddColumn(attempt => createKeyColumnPair(attempt).ConvertKey(StringHelpers.StringToBytes));
        }

        public Column GetColumn(string key, string columnName)
        {
            return implementation.GetColumn(StringHelpers.StringToBytes(key), StringHelpers.StringToBytes(columnName));
        }

        public bool TryGetColumn(string key, string columnName, out Column result)
        {
            return implementation.TryGetColumn(StringHelpers.StringToBytes(key), StringHelpers.StringToBytes(columnName),
                                               out result);
        }

        public void DeleteBatch(string key, IEnumerable<string> columnNames, long? timestamp = null)
        {
            implementation.DeleteBatch(StringHelpers.StringToBytes(key), columnNames.Select(StringHelpers.StringToBytes), timestamp);
        }

        public void BatchDelete(IEnumerable<KeyValuePair<string, IEnumerable<string>>> data, long? timestamp = null)
        {
            implementation.BatchDelete(data.Select(pair => new KeyValuePair<byte[], IEnumerable<byte[]>>(StringHelpers.StringToBytes(pair.Key), pair.Value.Select(StringHelpers.StringToBytes))), timestamp);
        }

        public void AddBatch(string key, IEnumerable<Column> columns)
        {
            implementation.AddBatch(StringHelpers.StringToBytes(key), columns);
        }

        public void AddBatch(Func<int, KeyColumnsPair<string>> createKeyColumnsPair)
        {
            implementation.AddBatch(attempt => createKeyColumnsPair(attempt).ConvertKey(StringHelpers.StringToBytes));
        }

        public void BatchInsert(IEnumerable<KeyValuePair<string, IEnumerable<Column>>> data)
        {
            var keyToColumns = new Dictionary<string, List<Column>>();
            foreach(var item in data)
            {
                List<Column> columns;
                if(!keyToColumns.TryGetValue(item.Key, out columns))
                {
                    columns = new List<Column>();
                    keyToColumns.Add(item.Key, columns);
                }
                columns.AddRange(item.Value);
            }
            implementation.BatchInsert(
                keyToColumns.Select(
                    item =>
                    new KeyValuePair<byte[], IEnumerable<Column>>(StringHelpers.StringToBytes(item.Key), item.Value)));
        }

        public Column[] GetColumns(string key, string exclusiveStartColumnName, int count)
        {
            return GetColumns(key, exclusiveStartColumnName, count, false);
        }

        public Column[] GetColumns(string key, string exclusiveStartColumnName, int count, bool reversed)
        {
            if (count == int.MaxValue) count--;
            if (count <= 0) return new Column[0];
            Column[] result = implementation.GetRow(StringHelpers.StringToBytes(key), StringHelpers.StringToBytes(exclusiveStartColumnName),
                                                    count + 1, reversed);
            if (result.Length == 0)
                return result;
            if (result[0].Name == exclusiveStartColumnName)
                result = result.Skip(1).ToArray();
            if (result.Length > count)
            {
                List<Column> list = result.ToList();
                list.RemoveAt(result.Length - 1);
                result = list.ToArray();
            }
            return result;
        }

        public Column[] GetColumns(string key, string startColumnName, string endColumnName, int count, bool reversed = false)
        {
            return implementation.GetRow(StringHelpers.StringToBytes(key), StringHelpers.StringToBytes(startColumnName),
                                         StringHelpers.StringToBytes(endColumnName), count, reversed);
        }

        public Column[] GetColumns(string key, string[] columnNames)
        {
            if (columnNames == null || columnNames.Length == int.MaxValue || columnNames.Length == 0)
            {
                return new Column[0];
            }
            var result = implementation.GetColumns(StringHelpers.StringToBytes(key), columnNames.Select(StringHelpers.StringToBytes).ToList());
            return result;
        }

        public IEnumerable<Column> GetRow(string key, int batchSize = 1000)
        {
            return enumerableFactory.GetColumnsEnumerator(key, batchSize, GetColumns);
        }

        public IEnumerable<Column> GetRow(string key, string exclusiveStartColumnName, int batchSize = 1000)
        {
            return enumerableFactory.GetColumnsEnumerator(key, batchSize, GetColumns, exclusiveStartColumnName);
        }

        public string[] GetKeys(string exclusiveStartKey, int count)
        {
            if(count == int.MaxValue) count--;
            if(count <= 0) return new string[0];
            var result = implementation.GetKeys(StringHelpers.StringToBytes(exclusiveStartKey), count + 1).Select(StringHelpers.BytesToString).ToArray();
            if(result.Length == 0) return result;
            if(result[0] == exclusiveStartKey) result = result.Skip(1).ToArray();
            if(result.Length > count) result = result.Take(count).ToArray();
            return result;
        }

        public IEnumerable<string> GetKeys(int batchSize = 1000)
        {
            return enumerableFactory.GetRowsEnumerator(batchSize, GetKeys);
        }

        public int GetCount(string key)
        {
            return implementation.GetCount(StringHelpers.StringToBytes(key));
        }

        public Dictionary<string, int> GetCounts(IEnumerable<string> keys)
        {
            var res = implementation.GetCounts(keys.Select(StringHelpers.StringToBytes));
            return res.ToDictionary(x => StringHelpers.BytesToString(x.Key), x => x.Value);
        }

        [Obsolete("Это устаревший метод. Надо пользоваться методом GetRowsExclusive")]
        public List<KeyValuePair<string, Column[]>> GetRows(IEnumerable<string> keys, string startColumnName, int count)
        {
            return
                implementation.GetRows(keys.Select(StringHelpers.StringToBytes),
                                       StringHelpers.StringToBytes(startColumnName), count).
                    Select(row => new KeyValuePair<string, Column[]>(StringHelpers.BytesToString(row.Key), row.Value)).
                    ToList();
        }

        public List<KeyValuePair<string, Column[]>> GetRowsExclusive(IEnumerable<string> keys, string exclusiveStartColumnName, int count)
        {
            var rows = implementation.GetRows(keys.Select(StringHelpers.StringToBytes), StringHelpers.StringToBytes(exclusiveStartColumnName), count + 1);
            var result = rows.Select(row =>
                {
                    var columns = row.Value;
                    if(columns != null && columns.Length > 0)
                    {
                        if(columns[0].Name == exclusiveStartColumnName)
                            columns = columns.Skip(1).ToArray();
                        if (columns.Length > count)
                            columns = columns.Take(count).ToArray();
                    }
                    return new KeyValuePair<string, Column[]>(StringHelpers.BytesToString(row.Key), columns);
                }).ToList();
            return result;
        }

        public string[] GetRowsWithColumnValue(int maximalCount, string key, byte[] value)
        {
            return GetRowsWhere(null, maximalCount, new[] {new IndexExpression {ColumnName = key, IndexOperator = IndexOperator.EQ, Value = value}}, new[] {key});
        }

        public void Truncate()
        {
            implementation.Truncate();
        }

        public string[] GetRowsWhere(string exclusiveStartKey, int count, IndexExpression[] conditions, string[] columns)
        {
            if(count == int.MaxValue) count--;
            if(count <= 0) return new string[0];
            var result = implementation.GetRowsWhere(StringHelpers.StringToBytes(exclusiveStartKey), count + 1, conditions, columns.Select(StringHelpers.StringToBytes).ToList()).Select(StringHelpers.BytesToString).ToArray();
            if(result.Length == 0) return result;
            if(result[0] == exclusiveStartKey) result = result.Skip(1).ToArray();
            if(result.Length > count) result = result.Take(count).ToArray();
            return result;
        }

        private readonly IColumnFamilyConnectionImplementation implementation;
        private readonly IEnumerableFactory enumerableFactory;
    }
}