using System.Collections.Generic;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Connections
{
    internal interface IEnumerableFactory
    {
        IEnumerable<string> GetRowKeysEnumerator(int batchSize);
        IEnumerable<Column> GetColumnsEnumerator(string key, int batchSize, string initialExclusiveStartColumnName);
    }
}