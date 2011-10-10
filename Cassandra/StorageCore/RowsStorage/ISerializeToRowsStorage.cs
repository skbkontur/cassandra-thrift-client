using System;
using System.Collections.Generic;

namespace SKBKontur.Cassandra.StorageCore.RowsStorage
{
    public interface ISerializeToRowsStorage
    {
        void Write<T>(string id, T obj) where T : class;
        void Write<T>(KeyValuePair<string, T>[] objects) where T : class;
        bool TryRead<T>(string id, out T result) where T : class;
        void Delete<T>(string id) where T : class;
        T Read<T>(string id) where T : class;
        T[] Read<T>(string[] ids) where T : class;
        T[] TryRead<T>(string[] ids) where T : class;
        T ReadOrCreate<T>(string id, Func<T> creator) where T : class;
        T[] ReadOrCreate<T>(string[] ids, Func<string, T> creator) where T : class;
        string[] GetIds<T>(string exclusiveStartId, int count) where T : class;

        string[] Search<TData, TTemplate>(string exclusiveStartKey, int count, TTemplate template)
            where TTemplate : class
            where TData : class;
    }
}