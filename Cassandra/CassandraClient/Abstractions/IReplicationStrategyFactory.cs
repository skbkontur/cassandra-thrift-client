using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface IReplicationStrategyFactory
    {
        IReplicationStrategy Create(KsDef ksDef);
    }
}