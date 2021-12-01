using System;
using System.Collections.Generic;
using System.Linq;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Helpers;
using SkbKontur.Cassandra.TimeBasedUuid.Bits;

namespace SkbKontur.Cassandra.ThriftClient.Connections
{
    internal class ColumnFamilyConnection : IColumnFamilyConnection
    {
        public ColumnFamilyConnection(IColumnFamilyConnectionImplementation implementation)
        {
            this.implementation = implementation;
            enumerableFactory = new EnumerableFactory(this);
        }

        public bool IsRowExist(string key)
        {
            return implementation.IsRowExist(StringExtensions.StringToBytes(key));
        }

        public void DeleteRows(string[] keys, long? timestamp = null, int batchSize = 1000)
        {
            var counts = GetCounts(keys);
            while (counts.Count > 0)
            {
                var rows = GetRowsExclusive(keys, null, batchSize);
                var d = rows.Select(row => new KeyValuePair<string, IEnumerable<string>>(row.Key, row.Value.Select(col => col.Name)));
                BatchDelete(d, timestamp);
                var newCounts = new Dictionary<string, int>();
                foreach (var keyValuePair in counts)
                {
                    if (keyValuePair.Value > batchSize)
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
            var rawKey = StringExtensions.StringToBytes(key);
            var rawColumnNames = new List<byte[]> {StringExtensions.StringToBytes(columnName)};
            implementation.DeleteBatch(rawKey, rawColumnNames, timestamp);
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
            if (!implementation.TryGetColumn(StringExtensions.StringToBytes(key), StringExtensions.StringToBytes(columnName), out var rawColumn))
                return false;
            result = rawColumn.ToColumn();
            return true;
        }

        public void DeleteBatch(string key, IEnumerable<string> columnNames, long? timestamp = null)
        {
            var rawKey = StringExtensions.StringToBytes(key);
            var rawColumnNames = columnNames.Select(StringExtensions.StringToBytes).ToList();
            implementation.DeleteBatch(rawKey, rawColumnNames, timestamp);
        }

        public void BatchDelete(IEnumerable<KeyValuePair<string, IEnumerable<string>>> data, long? timestamp = null)
        {
            var rawData = data.Select(pair => new KeyValuePair<byte[], List<byte[]>>(StringExtensions.StringToBytes(pair.Key), pair.Value.Select(StringExtensions.StringToBytes).ToList())).ToList();
            implementation.BatchDelete(rawData, timestamp);
        }

        public void AddBatch(string key, IEnumerable<Column> columns)
        {
            var rawKey = StringExtensions.StringToBytes(key);
            var rawColumns = columns.Select(column => column.ToRawColumn()).ToList();
            implementation.AddBatch(rawKey, rawColumns);
        }

        public void AddBatch(Func<int, KeyColumnsPair<string>> createKeyColumnsPair)
        {
            implementation.AddBatch(attempt => createKeyColumnsPair(attempt).Convert(StringExtensions.StringToBytes, ColumnExtensions.ToRawColumn));
        }

        public void BatchInsert(IEnumerable<KeyValuePair<string, IEnumerable<Column>>> data)
        {
            var keyToColumns = new Dictionary<byte[], List<RawColumn>>(ByteArrayComparer.Instance);
            foreach (var item in data)
            {
                var rawKey = StringExtensions.StringToBytes(item.Key);
                if (!keyToColumns.TryGetValue(rawKey, out var rawColumns))
                {
                    rawColumns = new List<RawColumn>();
                    keyToColumns.Add(rawKey, rawColumns);
                }
                rawColumns.AddRange(item.Value.Select(column => column.ToRawColumn()));
            }
            implementation.BatchInsert(keyToColumns.ToList());
        }

        public Column[] GetColumns(string key, string exclusiveStartColumnName, int count, bool reversed = false)
        {
            if (count == int.MaxValue) count--;
            if (count <= 0) return new Column[0];
            return implementation.GetRow(StringExtensions.StringToBytes(key), StringExtensions.StringToBytes(exclusiveStartColumnName), count + 1, reversed)
                                 .Select(ColumnExtensions.ToColumn)
                                 .Where(x => x.Name != exclusiveStartColumnName)
                                 .Take(count)
                                 .ToArray();
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
            if (columnNames == null || columnNames.Length == int.MaxValue || columnNames.Length == 0)
                return new Column[0];
            return implementation.GetColumns(StringExtensions.StringToBytes(key), columnNames.Select(StringExtensions.StringToBytes).ToList())
                                 .Select(ColumnExtensions.ToColumn)
                                 .ToArray();
        }

        public IEnumerable<Column> GetRow(string key, int batchSize = 1000)
        {
            return enumerableFactory.GetColumnsEnumerator(key, batchSize, initialExclusiveStartColumnName : null);
        }

        public IEnumerable<Column> GetRow(string key, string exclusiveStartColumnName, int batchSize = 1000)
        {
            return enumerableFactory.GetColumnsEnumerator(key, batchSize, exclusiveStartColumnName);
        }

        public string[] GetKeys(string exclusiveStartKey, int count)
        {
            if (count == int.MaxValue) count--;
            if (count <= 0) return new string[0];
            return implementation.GetKeys(StringExtensions.StringToBytes(exclusiveStartKey), count + 1)
                                 .Select(StringExtensions.BytesToString)
                                 .Where(key => key != exclusiveStartKey)
                                 .Take(count)
                                 .ToArray();
        }

        public IEnumerable<string> GetKeys(int batchSize = 1000)
        {
            return enumerableFactory.GetRowKeysEnumerator(batchSize);
        }

        public int GetCount(string key)
        {
            return implementation.GetCount(StringExtensions.StringToBytes(key));
        }

        public Dictionary<string, int> GetCounts(IEnumerable<string> keys)
        {
            var rawKeys = keys.Select(StringExtensions.StringToBytes).ToList();
            var res = implementation.GetCounts(rawKeys);
            return res.ToDictionary(x => StringExtensions.BytesToString(x.Key), x => x.Value);
        }

        public ICassandraConnectionParameters GetConnectionParameters()
        {
            return implementation.GetConnectionParameters();
        }

        public List<KeyValuePair<string, Column[]>> GetRegion(IEnumerable<string> keys, string startColumnName, string finishColumnName, int limitPerRow)
        {
            var rawKeys = keys.Select(StringExtensions.StringToBytes).ToList();
            var rawStartcolumnName = StringExtensions.StringToBytes(startColumnName);
            var rawFinishColumnName = StringExtensions.StringToBytes(finishColumnName);
            return implementation
                   .GetRegion(rawKeys, rawStartcolumnName, rawFinishColumnName, limitPerRow)
                   .Select(row => new KeyValuePair<string, Column[]>(StringExtensions.BytesToString(row.Key), row.Value.Select(ColumnExtensions.ToColumn).ToArray()))
                   .ToList();
        }

        public List<KeyValuePair<string, Column[]>> GetRowsExclusive(IEnumerable<string> keys, string exclusiveStartColumnName, int count)
        {
            if (count == int.MaxValue) count--;
            if (count <= 0) return keys.Select(row => new KeyValuePair<string, Column[]>(row, new Column[0])).ToList();

            var rawKeys = keys.Select(StringExtensions.StringToBytes).ToList();
            var rawStartColumnName = StringExtensions.StringToBytes(exclusiveStartColumnName);
            return implementation.GetRows(rawKeys, rawStartColumnName, count + 1)
                                 .Select(row => new KeyValuePair<string, Column[]>(StringExtensions.BytesToString(row.Key), row.Value.Select(ColumnExtensions.ToColumn)
                                                                                                                               .Where(column => column.Name != exclusiveStartColumnName)
                                                                                                                               .Take(count)
                                                                                                                               .ToArray()))
                                 .ToList();
        }

        public List<KeyValuePair<string, Column[]>> GetRows(IEnumerable<string> keys, string[] columnNames)
        {
            var rawKeys = keys.Select(StringExtensions.StringToBytes).ToList();
            var rawColumnNames = columnNames.Select(StringExtensions.StringToBytes).ToList();
            var rows = implementation.GetRows(rawKeys, rawColumnNames);
            return rows.Select(row => new KeyValuePair<string, Column[]>(StringExtensions.BytesToString(row.Key), row.Value.Select(ColumnExtensions.ToColumn).ToArray())).ToList();
        }

        public void Truncate()
        {
            implementation.Truncate();
        }

        private readonly IEnumerableFactory enumerableFactory;
        private readonly IColumnFamilyConnectionImplementation implementation;
    }
}