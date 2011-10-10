using System;

using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.FunctionalTests.Logger
{
    public class TestLogManager : ICassandraLogManager
    {
        public ICassandraLogger GetLogger(Type type)
        {
            return new TestLogger(type.Name);
        }
    }
}