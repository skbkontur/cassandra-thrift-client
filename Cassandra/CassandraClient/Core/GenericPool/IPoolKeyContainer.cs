namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    public interface IPoolKeyContainer<out TKey, out TReplicaKey>
    {
        TKey PoolKey { get; }

        TReplicaKey ReplicaKey { get; }
    }
}