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

        public void AddColumn(string key, Column column)
        {
            implementation.AddColumn(StringHelpers.StringToBytes(key), column);
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
            if(count == int.MaxValue) count--;
            if(count <= 0) return new Column[0];
            Column[] result = implementation.GetRow(StringHelpers.StringToBytes(key), StringHelpers.StringToBytes(exclusiveStartColumnName),
                                                    count + 1);
            if(result.Length == 0)
                return result;
            if(result[0].Name == exclusiveStartColumnName)
                result = result.Skip(1).ToArray();
            if(result.Length > count)
            {
                List<Column> list = result.ToList();
                list.RemoveAt(result.Length - 1);
                result = list.ToArray();
            }
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

        public List<KeyValuePair<string, Column[]>> GetRows(IEnumerable<string> keys, string startColumnName, int count)
        {
            return
                implementation.GetRows(keys.Select(StringHelpers.StringToBytes),
                                       StringHelpers.StringToBytes(startColumnName), count).
                    Select(row => new KeyValuePair<string, Column[]>(StringHelpers.BytesToString(row.Key), row.Value)).
                    ToList();
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