using System;

namespace SKBKontur.Cassandra.StorageCore
{
    public interface ICassandraLogManager
    {
        ICassandraLogger GetLogger(Type type);
    }
}