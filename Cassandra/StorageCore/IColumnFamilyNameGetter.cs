using System;

namespace StorageCore
{
    public interface IColumnFamilyNameGetter
    {
        string GetColumnFamilyName(Type type);
    }
}