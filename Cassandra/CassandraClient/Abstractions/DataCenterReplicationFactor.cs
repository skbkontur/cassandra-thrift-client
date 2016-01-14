namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class DataCenterReplicationFactor
    {
        public string DataCenterName { get; set; }
        public int ReplicationFactor { get; set; }
    }
}