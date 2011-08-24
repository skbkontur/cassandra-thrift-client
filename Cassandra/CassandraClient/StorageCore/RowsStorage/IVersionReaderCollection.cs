namespace CassandraClient.StorageCore.RowsStorage
{
    public interface IVersionReaderCollection
    {
        IVersionReader GetVersionReader(string version);
    }
}