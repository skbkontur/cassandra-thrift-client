using System;

namespace SKBKontur.Cassandra.StorageCore
{
    public interface IColumnFamilyTypeMapper
    {
        string GetColumnFamilyName(Type type);
        bool TryGetColumnFamilyName(Type type, out string columnFamilyName);
        Type GetColumnFamilyNameType(string columnFamilyName);
        Type TryGetColumnFamilyNameType(string columnFamilyName, out Type type);
    }
}