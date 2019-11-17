namespace SkbKontur.Cassandra.ThriftClient.Core.GenericPool
{
    internal interface ILiveness
    {
        bool IsAlive { get; }
    }
}