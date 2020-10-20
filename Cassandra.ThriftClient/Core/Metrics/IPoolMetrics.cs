namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal interface IPoolMetrics
    {
        void RecordAcquireNewConnection();
    }
}