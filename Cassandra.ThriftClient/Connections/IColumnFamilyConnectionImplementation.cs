using System;
using System.Collections.Generic;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Connections
{
    public interface IColumnFamilyConnectionImplementation
    {
        bool IsRowExist(byte[] key);
        int GetCount(byte[] key);
        Dictionary<byte[], int> GetCounts(List<byte[]> key);
        void DeleteRow(byte[] key, long? timestamp);
        void AddColumn(byte[] key, RawColumn column);
        void AddColumn(Func<int, KeyColumnPair<byte[], RawColumn>> createKeyColumnPair);
        void AddBatch(Func<int, KeyColumnsPair<byte[], RawColumn>> createKeyColumnsPair);
        List<KeyValuePair<byte[], List<RawColumn>>> GetRegion(List<byte[]> keys, byte[] startColumnName, byte[] finishColumnName, int limitPerRow);
        RawColumn GetColumn(byte[] key, byte[] columnName);
        bool TryGetColumn(byte[] key, byte[] columnName, out RawColumn result);
        void DeleteBatch(byte[] key, List<byte[]> columnNames, long? timestamp = null);
        void BatchDelete(List<KeyValuePair<byte[], List<byte[]>>> data, long? timestamp = null);
        void AddBatch(byte[] key, List<RawColumn> columns);
        void BatchInsert(List<KeyValuePair<byte[], List<RawColumn>>> data);
        List<KeyValuePair<byte[], List<RawColumn>>> GetRows(List<byte[]> keys, byte[] startColumnName, int count);
        List<KeyValuePair<byte[], List<RawColumn>>> GetRows(List<byte[]> keys, List<byte[]> columnNames);
        void Truncate();
        List<RawColumn> GetRow(byte[] key, byte[] startColumnName, int count, bool reversed);
        List<RawColumn> GetRow(byte[] key, byte[] startColumnName, byte[] endColumnName, int count, bool reversed);
        List<RawColumn> GetColumns(byte[] key, List<byte[]> columnNames);
        List<byte[]> GetKeys(byte[] startKey, int count);
        ICassandraConnectionParameters GetConnectionParameters();
    }
}