namespace SkbKontur.Cassandra.ThriftClient.Core.GenericPool
{
    internal interface IPoolKeyContainer<out TKey, out TReplicaKey>
    {
        TKey PoolKey { get; }

        TReplicaKey ReplicaKey { get; }
    }
}