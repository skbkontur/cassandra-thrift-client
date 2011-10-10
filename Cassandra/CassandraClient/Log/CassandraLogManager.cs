using System;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.Log
{
    public class CassandraLogManager : ICassandraLogManager
    {
        public ICassandraLogger GetLogger(Type type)
        {
            return new CassandraLogger(LogManager.GetLogger(type));
        }
    }
}