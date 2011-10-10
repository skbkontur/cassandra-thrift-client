using System;

namespace SKBKontur.Cassandra.CassandraClient.Log
{
    public interface ICassandraLogManager
    {
        ICassandraLogger GetLogger(Type type);
    }
}