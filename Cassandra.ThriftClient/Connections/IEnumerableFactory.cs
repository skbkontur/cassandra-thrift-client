using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    internal interface IEnumerableFactory
    {
        IEnumerable<string> GetRowKeysEnumerator(int batchSize);
        IEnumerable<Column> GetColumnsEnumerator(string key, int batchSize, string initialExclusiveStartColumnName);
    }
}