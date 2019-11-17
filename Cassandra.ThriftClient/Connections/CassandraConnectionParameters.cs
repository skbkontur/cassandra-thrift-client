using SkbKontur.Cassandra.ThriftClient.Clusters;

namespace SkbKontur.Cassandra.ThriftClient.Connections
{
    public interface ICassandraConnectionParameters
    {
        int Attempts { get; }
        int Timeout { get; }
    }

    public class CassandraConnectionParameters : ICassandraConnectionParameters
    {
        public CassandraConnectionParameters(ICassandraClusterSettings cassandraClusterSettings)
        {
            this.cassandraClusterSettings = cassandraClusterSettings;
        }

        public int Attempts => cassandraClusterSettings.Attempts;
        public int Timeout => cassandraClusterSettings.Timeout;

        private readonly ICassandraClusterSettings cassandraClusterSettings;
    }
}