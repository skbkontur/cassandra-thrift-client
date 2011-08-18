using System.Collections.Generic;

namespace CassandraClient.StorageCore.RowsStorage
{
    public interface ISerializeToRowsStorage
    {
        void Write<T>(string id, T obj) where T : class;
        void Write<T>(KeyValuePair<string, T>[] objects) where T : class;
        bool TryRead<T>(string id, out T result) where T : class;
        void Delete<T>(string id) where T : class;
        T Read<T>(string id) where T : class;
        T[] Read<T>(string[] ids) where T : class;
        T ReadOrCreate<T>(string id) where T : class, new();
        T[] ReadOrCreate<T>(string[] ids) where T : class, new();

        string[] Search<TData, TTemplate>(TTemplate template)
            where TTemplate : class
            where TData : class;
    }
}