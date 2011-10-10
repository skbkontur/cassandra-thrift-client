namespace SKBKontur.Cassandra.StorageCore
{
    public interface ICassandraCoreSettings
    {
        int MaximalColumnsCount { get; }
        int MaximalRowsCount { get; }
        string KeyspaceName { get; }
    }
}