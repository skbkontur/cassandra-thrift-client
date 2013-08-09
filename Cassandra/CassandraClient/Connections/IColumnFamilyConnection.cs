using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public interface IColumnFamilyConnection
    {
        bool IsRowExist(string key);
        void DeleteRows(string[] keys, long? timestamp = null, int batchSize = 1000);
        void DeleteRow(string key, long? timestamp = null);
        void AddColumn(string key, Column column);

        void AddColumn(Func<int, KeyColumnPair<string>> createKeyColumnPair);

        Column GetColumn(string key, string columnName);
        bool TryGetColumn(string key, string columnName, out Column result);
        void DeleteBatch(string key, IEnumerable<string> columnNames, long? timestamp = null);
        void AddBatch(string key, IEnumerable<Column> columns);
        void AddBatch(Func<int, KeyColumnsPair<string>> createKeyColumnsPair);
        void BatchInsert(IEnumerable<KeyValuePair<string, IEnumerable<Column>>> data);
        void BatchDelete(IEnumerable<KeyValuePair<string, IEnumerable<string>>> data, long? timestamp = null);

        [Obsolete("Это устаревший метод. Надо пользоваться методом GetRowsExclusive")]
        List<KeyValuePair<string, Column[]>> GetRows(IEnumerable<string> keys, string startColumnName, int count);

        List<KeyValuePair<string, Column[]>> GetRowsExclusive(IEnumerable<string> keys, string exclusiveStartColumnName, int count);
        string[] GetRowsWhere(string exclusiveStartKey, int count, IndexExpression[] conditions, string[] columns);
        string[] GetRowsWithColumnValue(int maximalCount, string key, byte[] value);
        void Truncate();
        Column[] GetColumns(string key, string exclusiveStartColumnName, int count);
        Column[] GetColumns(string key, string exclusiveStartColumnName, int count, bool reversed);
        Column[] GetColumns(string key, string startColumnName, string endColumnName, int count, bool reversed = false);
        Column[] GetColumns(string key, string[] columnNames);
        IEnumerable<Column> GetRow(string key, int batchSize = 1000);
        IEnumerable<Column> GetRow(string key, string exclusiveStartColumnName, int batchSize = 1000);
        string[] GetKeys(string exclusiveStartKey, int count);
        IEnumerable<string> GetKeys(int batchSize = 1000);
        int GetCount(string key);
        Dictionary<string, int> GetCounts(IEnumerable<string> keys);
    }
}