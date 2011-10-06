using System;
using System.Collections.Generic;

using CassandraClient.Abstractions;

namespace CassandraClient.Connections
{
    public interface IEnumerableFactory
    {
        IEnumerable<string> GetRowsEnumerator(int bulkSize, Func<string, int, string[]> getRows);
        IEnumerable<Column> GetColumnsEnumerator(string key, int bulkSize, Func<string, string, int, Column[]> getColumns);
    }
}