namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    public interface ILiveness
    {
        bool IsAlive { get; }
    }
}