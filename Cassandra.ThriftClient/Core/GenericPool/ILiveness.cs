namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    internal interface ILiveness
    {
        bool IsAlive { get; }
    }
}