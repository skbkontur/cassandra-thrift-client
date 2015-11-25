using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public interface IColumnFamilyConnectionImplementation<T>
    {
        bool IsRowExist(byte[] key);
        int GetCount(byte[] key);
        Dictionary<byte[], int> GetCounts(IEnumerable<byte[]> key);
        void DeleteRow(byte[] key, long? timestamp);
        void AddColumn(byte[] key, T column);

        void AddColumn(Func<int, KeyColumnPair<byte[], T>> createKeyColumnPair);
        void AddBatch(Func<int, KeyColumnsPair<byte[], T>> createKeyColumnsPair);

        List<KeyValuePair<byte[], T[]>> GetRegion(IEnumerable<byte[]> keys, byte[] startColumnName, byte[] finishColumnName, int limitPerRow);

        T GetColumn(byte[] key, byte[] columnName);
        bool TryGetColumn(byte[] key, byte[] columnName, out T result);
        void DeleteBatch(byte[] key, IEnumerable<byte[]> columnNames, long? timestamp = null);
        void BatchDelete(IEnumerable<KeyValuePair<byte[], IEnumerable<byte[]>>> data, long? timestamp = null);
        void AddBatch(byte[] key, IEnumerable<T> columns);
        void BatchInsert(IEnumerable<KeyValuePair<byte[], IEnumerable<T>>> data);
        List<KeyValuePair<byte[], T[]>> GetRows(IEnumerable<byte[]> keys, byte[] startColumnName, int count);
        List<KeyValuePair<byte[], T[]>> GetRows(IEnumerable<byte[]> keys, List<byte[]> columnNames);
        List<byte[]> GetRowsWhere(byte[] startKey, int maximalCount, IEnumerable<GeneralIndexExpression> conditions, List<byte[]> columns);
        void Truncate();
        T[] GetRow(byte[] key, byte[] startColumnName, int count, bool reversed);
        T[] GetRow(byte[] key, byte[] startColumnName, byte[] endColumnName, int count, bool reversed);
        T[] GetColumns(byte[] key, List<byte[]> columnNames);
        List<byte[]> GetKeys(byte[] startKey, int count);

        ICassandraConnectionParameters GetConnectionParameters();
    }
}