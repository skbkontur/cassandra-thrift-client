namespace SKBKontur.Cassandra.StorageCore
{
    public interface IIdSetRepository
    {
        void Write(params string[] ids);
        string[] Read(int maxCount, string startId);
    }
}