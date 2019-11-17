using Apache.Cassandra;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    internal interface IReplicationStrategyFactory
    {
        IReplicationStrategy Create(KsDef ksDef);
    }
}