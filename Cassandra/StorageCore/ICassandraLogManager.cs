using System;

namespace StorageCore
{
    public interface ICassandraLogManager
    {
        ICassandraLogger GetLogger(Type type);
    }
}