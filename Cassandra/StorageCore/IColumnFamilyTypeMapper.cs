using System;

namespace SKBKontur.Cassandra.StorageCore
{
    public interface IColumnFamilyTypeMapper
    {
        string GetColumnFamilyName(Type type);
        bool TryGetColumnFamilyName(Type type, out string columnFamilyName);
        Type GetColumnFamilyType(string columnFamilyName);
        bool TryGetColumnFamilyType(string columnFamilyName, out Type type);
    }
}