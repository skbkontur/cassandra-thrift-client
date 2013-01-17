using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public interface IColumnFamilyConnectionImplementation
    {
        bool IsRowExist(byte[] key);
        int GetCount(byte[] key);
        Dictionary<byte[], int> GetCounts(IEnumerable<byte[]> key);
        void DeleteRow(byte[] key, long? timestamp);
        void AddColumn(byte[] key, Column column);
        Column GetColumn(byte[] key, byte[] columnName);
        bool TryGetColumn(byte[] key, byte[] columnName, out Column result);
        void DeleteBatch(byte[] key, IEnumerable<byte[]> columnNames, long? timestamp = null);
        void BatchDelete(IEnumerable<KeyValuePair<byte[], IEnumerable<byte[]>>> data, long? timestamp = null);
        void AddBatch(byte[] key, IEnumerable<Column> columns);
        void BatchInsert(IEnumerable<KeyValuePair<byte[], IEnumerable<Column>>> data);
        List<KeyValuePair<byte[], Column[]>> GetRows(IEnumerable<byte[]> keys, byte[] startColumnName, int count);
        List<byte[]> GetRowsWhere(byte[] startKey, int maximalCount, IndexExpression[] conditions, List<byte[]> columns);
        void Truncate();
        Column[] GetRow(byte[] key, byte[] startColumnName, int count);
        List<byte[]> GetKeys(byte[] startKey, int count);
    }
}