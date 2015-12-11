using System;
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
        void AddColumn(byte[] key, RawColumn column);
        void AddColumn(Func<int, KeyColumnPair<byte[], RawColumn>> createKeyColumnPair);
        void AddBatch(Func<int, KeyColumnsPair<byte[], RawColumn>> createKeyColumnsPair);
        List<KeyValuePair<byte[], IEnumerable<RawColumn>>> GetRegion(IEnumerable<byte[]> keys, byte[] startColumnName, byte[] finishColumnName, int limitPerRow);
        RawColumn GetColumn(byte[] key, byte[] columnName);
        bool TryGetColumn(byte[] key, byte[] columnName, out RawColumn result);
        void DeleteBatch(byte[] key, IEnumerable<byte[]> columnNames, long? timestamp = null);
        void BatchDelete(IEnumerable<KeyValuePair<byte[], IEnumerable<byte[]>>> data, long? timestamp = null);
        void AddBatch(byte[] key, IEnumerable<RawColumn> columns);
        void BatchInsert(IEnumerable<KeyValuePair<byte[], IEnumerable<RawColumn>>> data);
        List<KeyValuePair<byte[], IEnumerable<RawColumn>>> GetRows(IEnumerable<byte[]> keys, byte[] startColumnName, int count);
        List<KeyValuePair<byte[], IEnumerable<RawColumn>>> GetRows(IEnumerable<byte[]> keys, List<byte[]> columnNames);
        List<byte[]> GetRowsWhere(byte[] startKey, int maximalCount, IEnumerable<RawIndexExpression> conditions, List<byte[]> columns);
        void Truncate();
        IEnumerable<RawColumn> GetRow(byte[] key, byte[] startColumnName, int count, bool reversed);
        IEnumerable<RawColumn> GetRow(byte[] key, byte[] startColumnName, byte[] endColumnName, int count, bool reversed);
        IEnumerable<RawColumn> GetColumns(byte[] key, List<byte[]> columnNames);
        List<byte[]> GetKeys(byte[] startKey, int count);
        ICassandraConnectionParameters GetConnectionParameters();
    }
}