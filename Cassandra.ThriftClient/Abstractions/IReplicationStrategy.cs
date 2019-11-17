using System.Collections.Generic;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public interface IReplicationStrategy
    {
        string Name { get; }
        Dictionary<string, string> StrategyOptions { get; }
    }
}