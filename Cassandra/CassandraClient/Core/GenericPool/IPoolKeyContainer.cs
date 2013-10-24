namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    public interface IPoolKeyContainer<out TKey>
    {
        TKey PoolKey { get; }
    }
}