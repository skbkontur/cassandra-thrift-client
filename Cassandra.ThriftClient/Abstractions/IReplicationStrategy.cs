using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public interface IReplicationStrategy
    {
        string Name { get; }
        Dictionary<string, string> StrategyOptions { get; }
    }
}