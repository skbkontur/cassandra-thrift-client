using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    internal interface IEnumerableFactory
    {
        IEnumerable<string> GetRowsEnumerator(int bulkSize, Func<string, int, string[]> getRows, string initialStartKey = null);
        IEnumerable<Column> GetColumnsEnumerator(string key, int bulkSize, Func<string, string, int, Column[]> getColumns, string initialStartKey = null);
    }
}