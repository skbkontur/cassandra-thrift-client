using System.Collections.Generic;
using System.Linq;

using CassandraClient.Abstractions;
using CassandraClient.Helpers;

namespace CassandraClient.Connections
{
    public class ColumnFamilyConnection : IColumnFamilyConnection
    {
        public ColumnFamilyConnection(IColumnFamilyConnectionImplementation implementation)
        {
            this.implementation = implementation;
        }

        #region IColumnFamilyConnection Members

        public void Dispose()
        {
            implementation.Dispose();
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

        public void BatchDelete(IEnumerable<KeyValuePair<string, IEnumerable<string>>> data)
        {
            implementation.BatchDelete(data.Select(pair => new KeyValuePair<byte[], IEnumerable<byte[]>>(StringHelpers.StringToBytes(pair.Key), pair.Value.Select(StringHelpers.StringToBytes))));
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

        public Column[] GetRow(string key, string exclusiveStartColumnName, int count)
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

        public string[] GetKeys(string exclusiveStartKey, int count)
        {
            if (count == int.MaxValue) count--;
            if (count <= 0) return new string[0];
            var result = implementation.GetKeys(StringHelpers.StringToBytes(exclusiveStartKey), count + 1).Select(StringHelpers.BytesToString).ToArray();
            if (result.Length == 0) return result;
            if (result[0] == exclusiveStartKey) result = result.Skip(1).ToArray();
            if (result.Length > count) result = result.Take(count).ToArray();
            return result;
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
            if (count == int.MaxValue) count--;
            if (count <= 0) return new string[0];
            var result = implementation.GetRowsWhere(StringHelpers.StringToBytes(exclusiveStartKey), count + 1, conditions.Select(cond => cond.ToAquilesIndexExpression()).ToArray(), columns.Select(StringHelpers.StringToBytes).ToList()).Select(StringHelpers.BytesToString).ToArray();
            if (result.Length == 0) return result;
            if (result[0] == exclusiveStartKey) result = result.Skip(1).ToArray();
            if (result.Length > count) result = result.Take(count).ToArray();
            return result;
        }

        #endregion

        private readonly IColumnFamilyConnectionImplementation implementation;
    }
}