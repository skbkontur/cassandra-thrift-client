using CassandraClient.Abstractions;

namespace CassandraClient.StorageCore.RowsStorage
{
    public interface IVersionReader
    {
        bool TryReadObject<T>(Column[] allColumns, Column[] specialColumns, out T result) where T : class;
    }
}