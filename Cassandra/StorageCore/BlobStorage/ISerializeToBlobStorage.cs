namespace SKBKontur.Cassandra.StorageCore.BlobStorage
{
    public interface ISerializeToBlobStorage
    {
        T Read<T>(string id) where T : class;
        bool TryRead<T>(string id, out T result) where T : class;
        void Write<T>(string id, T data) where T : class;
        void Delete<T>(string id) where T : class;
    }
}