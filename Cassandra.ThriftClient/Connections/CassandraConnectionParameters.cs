using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Cassandra.CassandraClient.Connections
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

        public int Attempts { get { return cassandraClusterSettings.Attempts; } }
        public int Timeout { get { return cassandraClusterSettings.Timeout; } }

        private readonly ICassandraClusterSettings cassandraClusterSettings;
    }
}