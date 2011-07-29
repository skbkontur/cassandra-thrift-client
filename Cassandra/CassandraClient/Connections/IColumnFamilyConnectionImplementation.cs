using System;
using System.Collections.Generic;

using Aquiles.Model;

using CassandraClient.Abstractions;

namespace CassandraClient.Connections
{
    public interface IColumnFamilyConnectionImplementation : IDisposable
    {
        void AddColumn(byte[] key, Column column);
        Column GetColumn(byte[] key, byte[] columnName);
        bool TryGetColumn(byte[] key, byte[] columnName, out Column result);
        void DeleteBatch(byte[] key, IEnumerable<byte[]> columnNames);
        void BatchDelete(IEnumerable<KeyValuePair<byte[], IEnumerable<byte[]>>> data);
        void AddBatch(byte[] key, IEnumerable<Column> columns);
        void BatchInsert(IEnumerable<KeyValuePair<byte[], IEnumerable<Column>>> data);
        Column[] GetRow(byte[] key, byte[] startColumnName, int count);
        List<KeyValuePair<byte[], Column[]>> GetRows(IEnumerable<byte[]> keys, byte[] startColumnName, int count);
        List<byte[]> GetRowsWhere(int maximalCount, AquilesIndexExpression[] conditions, List<byte[]> columns);
        void Truncate();
    }
}