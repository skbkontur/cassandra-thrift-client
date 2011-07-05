using CassandraClient.Abstractions;

namespace CassandraClient.Clusters
{
    public class CassandraClusterSettings : ICassandraClusterSettings
    {
        public string Name { get; set; }
        public ConsistencyLevel ClusterReadConsistencyLevel { get; set; }
        public ConsistencyLevel ClusterWriteConsistencyLevel { get; set; }
        public ConsistencyLevel ColumnFamilyReadConsistencyLevel { get; set; }
        public ConsistencyLevel ColumnFamilyWriteConsistencyLevel { get; set; }
    }
}