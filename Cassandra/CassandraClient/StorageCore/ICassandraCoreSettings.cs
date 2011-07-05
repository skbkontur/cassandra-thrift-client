namespace CassandraClient.StorageCore
{
    public interface ICassandraCoreSettings
    {
        int MaximalColumnsCount { get; }
        int MaximalRowsCount { get; }
        string ClusterName { get; }
        string KeyspaceName { get; }
    }
}