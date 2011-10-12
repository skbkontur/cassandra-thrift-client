using System;

namespace SKBKontur.Cassandra.StorageCore
{
    public interface IColumnFamilyNameGetter
    {
        string GetColumnFamilyName(Type type);
        bool TryGetColumnFamilyName(Type type, out string columnFamilyName);
    }
}