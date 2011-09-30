using CassandraClient.Abstractions;

namespace CassandraClient.StorageCore.RowsStorage
{
    public interface IObjectReader
    {
        bool TryReadObject<T>(Column[] columns, out T result) where T : class;
    }
}