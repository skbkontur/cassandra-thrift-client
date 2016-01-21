using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface IReplicationStrategyFactory
    {
        IReplicationStrategy Create(string strategyName, Dictionary<string, string> strategyOptions);
    }
}