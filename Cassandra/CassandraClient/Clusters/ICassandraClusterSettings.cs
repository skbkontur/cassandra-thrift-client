using CassandraClient.Abstractions;

namespace CassandraClient.Clusters
{
    public interface ICassandraClusterSettings
    {
        string Name { get; }
        ConsistencyLevel ClusterReadConsistencyLevel { get; }
        ConsistencyLevel ClusterWriteConsistencyLevel { get; }
        ConsistencyLevel ColumnFamilyReadConsistencyLevel { get; }
        ConsistencyLevel ColumnFamilyWriteConsistencyLevel { get; }
    }
}