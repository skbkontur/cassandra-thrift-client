using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    internal class ColumnFamilyConnection : IColumnFamilyConnection
    {
        public ColumnFamilyConnection(IColumnFamilyConnectionImplementation implementation, IEnumerableFactory enumerableFactory)
        {
            this.implementation = implementation;
            this.enumerableFactory = enumerableFactory;
        }

        public bool IsRowExist(string key)
        {
            return implementation.IsRowExist(StringExtensions.StringToBytes(key));
        }

        public void DeleteRows(string[] keys, long? timestamp = null, int batchSize = 1000)
        {
            var counts = GetCounts(keys);
            while(counts.Count > 0)
            {
                var rows = GetRowsExclusive(keys, null, batchSize);
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
            implementation.DeleteRow(StringExtensions.StringToBytes(key), timestamp);
        }

        public void DeleteColumn(string key, string columnName, long? timestamp = null)
        {
            implementation.DeleteBatch(StringExtensions.StringToBytes(key), new[] {StringExtensions.StringToBytes(columnName)}, timestamp);
        }

        public void AddColumn(string key, Column column)
        {
            implementation.AddColumn(StringExtensions.StringToBytes(key), column.ToRawColumn());
        }

        public void AddColumn(Func<int, KeyColumnPair<string>> createKeyColumnPair)
        {
            implementation.AddColumn(attempt => createKeyColumnPair(attempt).Convert(StringExtensions.StringToBytes, ColumnExtensions.ToRawColumn));
        }

        public Column GetColumn(string key, string columnName)
        {
            return implementation.GetColumn(StringExtensions.StringToBytes(key), StringExtensions.StringToBytes(columnName)).ToColumn();
        }

        public bool TryGetColumn(string key, string columnName, out Column result)
        {
            result = null;
            RawColumn rawColumn;
            if(!implementation.TryGetColumn(StringExtensions.StringToBytes(key), StringExtensions.StringToBytes(columnName), out rawColumn))
                return false;
            result = rawColumn.ToColumn();
            return true;
        }

        public void DeleteBatch(string key, IEnumerable<string> columnNames, long? timestamp = null)
        {
            implementation.DeleteBatch(StringExtensions.StringToBytes(key), columnNames.Select(StringExtensions.StringToBytes), timestamp);
        }

        public void BatchDelete(IEnumerable<KeyValuePair<string, IEnumerable<string>>> data, long? timestamp = null)
        {
            implementation.BatchDelete(data.Select(pair => new KeyValuePair<byte[], IEnumerable<byte[]>>(StringExtensions.StringToBytes(pair.Key), pair.Value.Select(StringExtensions.StringToBytes))), timestamp);
        }

        public void AddBatch(string key, IEnumerable<Column> columns)
        {
            implementation.AddBatch(StringExtensions.StringToBytes(key), columns.Select(column => column.ToRawColumn()));
        }

        public void AddBatch(Func<int, KeyColumnsPair<string>> createKeyColumnsPair)
        {
            implementation.AddBatch(attempt => createKeyColumnsPair(attempt).Convert(StringExtensions.StringToBytes, ColumnExtensions.ToRawColumn));
        }

        public void BatchInsert(IEnumerable<KeyValuePair<string, IEnumerable<Column>>> data)
        {
            var keyToColumns = new Dictionary<string, List<RawColumn>>();
            foreach(var item in data)
            {
                List<RawColumn> columns;
                if(!keyToColumns.TryGetValue(item.Key, out columns))
                {
                    columns = new List<RawColumn>();
                    keyToColumns.Add(item.Key, columns);
                }
                columns.AddRange(item.Value.Select(column => column.ToRawColumn()));
            }
            implementation.BatchInsert(
                keyToColumns.Select(
                    item =>
                    new KeyValuePair<byte[], IEnumerable<RawColumn>>(StringExtensions.StringToBytes(item.Key), item.Value)));
        }

        public Column[] GetColumns(string key, string exclusiveStartColumnName, int count)
        {
            return GetColumns(key, exclusiveStartColumnName, count, false);
        }

        public Column[] GetColumns(string key, string exclusiveStartColumnName, int count, bool reversed)
        {
            if(count == int.MaxValue) count--;
            if(count <= 0) return new Column[0];
            var rawColumns = implementation.GetRow(StringExtensions.StringToBytes(key), StringExtensions.StringToBytes(exclusiveStartColumnName),
                                                   count + 1, reversed);
            return GetColumnsFromRawColumns(rawColumns, exclusiveStartColumnName, count);
        }

        public Column[] GetColumns(string key, string startColumnName, string endColumnName, int count, bool reversed = false)
        {
            return implementation.GetRow(StringExtensions.StringToBytes(key), StringExtensions.StringToBytes(startColumnName),
                                         StringExtensions.StringToBytes(endColumnName), count, reversed)
                                 .Select(ColumnExtensions.ToColumn)
                                 .ToArray();
        }

        public Column[] GetColumns(string key, string[] columnNames)
        {
            if(columnNames == null || columnNames.Length == int.MaxValue || columnNames.Length == 0)
                return new Column[0];
            return implementation.GetColumns(StringExtensions.StringToBytes(key), columnNames.Select(StringExtensions.StringToBytes).ToList())
                                 .Select(ColumnExtensions.ToColumn)
                                 .ToArray();
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
            var result = implementation.GetKeys(StringExtensions.StringToBytes(exclusiveStartKey), count + 1).Select(StringExtensions.BytesToString).ToArray();
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
            return implementation.GetCount(StringExtensions.StringToBytes(key));
        }

        public Dictionary<string, int> GetCounts(IEnumerable<string> keys)
        {
            var res = implementation.GetCounts(keys.Select(StringExtensions.StringToBytes));
            return res.ToDictionary(x => StringExtensions.BytesToString(x.Key), x => x.Value);
        }

        public ICassandraConnectionParameters GetConnectionParameters()
        {
            return implementation.GetConnectionParameters();
        }

        [Obsolete("Это устаревший метод. Надо пользоваться методом GetRowsExclusive")]
        public List<KeyValuePair<string, Column[]>> GetRows(IEnumerable<string> keys, string startColumnName, int count)
        {
            return
                implementation.GetRows(keys.Select(StringExtensions.StringToBytes), StringExtensions.StringToBytes(startColumnName), count)
                              .Select(row => new KeyValuePair<string, Column[]>(StringExtensions.BytesToString(row.Key), row.Value.Select(ColumnExtensions.ToColumn).ToArray()))
                              .ToList();
        }

        public List<KeyValuePair<string, Column[]>> GetRegion(IEnumerable<string> keys, string startColumnName, string finishColumnName, int limitPerRow)
        {
            return implementation
                .GetRegion(keys.Select(StringExtensions.StringToBytes), StringExtensions.StringToBytes(startColumnName), StringExtensions.StringToBytes(finishColumnName), limitPerRow)
                .Select(row => new KeyValuePair<string, Column[]>(StringExtensions.BytesToString(row.Key), row.Value.Select(ColumnExtensions.ToColumn).ToArray()))
                .ToList();
        }

        public List<KeyValuePair<string, Column[]>> GetRowsExclusive(IEnumerable<string> keys, string exclusiveStartColumnName, int count)
        {
            var rows = implementation.GetRows(keys.Select(StringExtensions.StringToBytes), StringExtensions.StringToBytes(exclusiveStartColumnName), count + 1);
            return rows.Select(row => new KeyValuePair<string, Column[]>(StringExtensions.BytesToString(row.Key),
                                                                         GetColumnsFromRawColumns(row.Value, exclusiveStartColumnName, count)))
                       .ToList();
        }

        public List<KeyValuePair<string, Column[]>> GetRows(IEnumerable<string> keys, string[] columnNames)
        {
            var rows = implementation.GetRows(keys.Select(StringExtensions.StringToBytes), columnNames.Select(StringExtensions.StringToBytes).ToList());
            return rows.Select(row => new KeyValuePair<string, Column[]>(StringExtensions.BytesToString(row.Key), row.Value.Select(ColumnExtensions.ToColumn).ToArray())).ToList();
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
            var result = implementation.GetRowsWhere(StringExtensions.StringToBytes(exclusiveStartKey), count + 1, conditions.Select(IndexExpressionExtensions.ToGeneralIndesExpression), columns.Select(StringExtensions.StringToBytes).ToList()).Select(StringExtensions.BytesToString).ToArray();
            if(result.Length == 0) return result;
            if(result[0] == exclusiveStartKey) result = result.Skip(1).ToArray();
            if(result.Length > count) result = result.Take(count).ToArray();
            return result;
        }

        private static Column[] GetColumnsFromRawColumns(IEnumerable<RawColumn> rawColumns, string exclusiveStartColumnName, int count)
        {
            if(rawColumns == null)
                return new Column[0];

            var result = new List<Column>();
            var isFirst = true;
            var currentCount = 0;
            foreach(var rawColumm in rawColumns)
            {
                if(currentCount == count)
                    break;

                if(isFirst && StringExtensions.BytesToString(rawColumm.Name) == exclusiveStartColumnName)
                {
                    isFirst = false;
                    continue;
                }

                result.Add(rawColumm.ToColumn());
                currentCount++;
            }
            return result.ToArray();
        }

        private readonly IEnumerableFactory enumerableFactory;
        private readonly IColumnFamilyConnectionImplementation implementation;
    }
}