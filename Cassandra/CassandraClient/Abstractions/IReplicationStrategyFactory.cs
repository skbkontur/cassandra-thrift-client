using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public interface IReplicationStrategyFactory
    {
        IReplicationStrategy Create(string strategyName, Dictionary<string, string> strategyOptions);
    }
}