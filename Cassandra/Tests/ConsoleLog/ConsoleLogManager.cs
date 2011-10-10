using System;

using SKBKontur.Cassandra.CassandraClient.Log;

namespace Cassandra.Tests.ConsoleLog
{
    public class ConsoleLogManager : ICassandraLogManager
    {
        public ICassandraLogger GetLogger(Type type)
        {
            return new ConsoleLogger(type.Name);
        }
    }
}