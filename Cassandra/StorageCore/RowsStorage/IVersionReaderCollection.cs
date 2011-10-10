namespace SKBKontur.Cassandra.StorageCore.RowsStorage
{
    public interface IVersionReaderCollection
    {
        IVersionReader GetVersionReader(string version);
    }
}