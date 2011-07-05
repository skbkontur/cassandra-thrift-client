using System;

namespace CassandraClient.StorageCore
{
    public interface ICassandraLogManager
    {
        ICassandraLogger GetLogger(Type type);
    }
}