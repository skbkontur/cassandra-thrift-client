﻿namespace CassandraClient.StorageCore.KeyValueTables
{
    public interface IKeyValueIndexTable
    {
        void AddLinks(params KeyToValue[] links);
        void AddLink(string key, string value);
        string[] GetKeys(string value);
        string[] GetValues(string key);
    }
}