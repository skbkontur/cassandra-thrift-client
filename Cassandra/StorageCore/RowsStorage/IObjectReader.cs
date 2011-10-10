using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.StorageCore.RowsStorage
{
    public interface IObjectReader
    {
        bool TryReadObject<T>(Column[] columns, out T result) where T : class;
    }
}