namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class DataCenterReplicationFactor
    {
        public string DataCenterName { get; private set; }
        public int ReplicationFactor { get; private set; }

        public DataCenterReplicationFactor(string dataCenterName, int replicationFactor)
        {
            DataCenterName = dataCenterName;
            ReplicationFactor = replicationFactor;
        }
    }
}