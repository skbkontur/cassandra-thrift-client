using System;

namespace SKBKontur.Cassandra.StorageCore
{
    public interface IColumnFamilyNameGetter
    {
        string GetColumnFamilyName(Type type);
    }
}