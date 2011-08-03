using System;

namespace CassandraClient.StorageCore
{
    public interface IColumnFamilyNameGetter
    {
        string GetColumnFamilyName(Type type);
    }
}