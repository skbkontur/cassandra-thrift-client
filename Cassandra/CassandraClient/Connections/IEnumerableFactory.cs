using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public interface IEnumerableFactory
    {
        IEnumerable<byte[]> GetRowsEnumerator(int batchSize, byte[] exclusiveInitialStartKey = null);
        IEnumerable<RawColumn> GetColumnsEnumerator(byte[] key, int batchSize, byte[] exclusiveInitialStartKey = null);
    }
}