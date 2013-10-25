using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    public class AllItemsIsDeadExceptions : Exception
    {
        public AllItemsIsDeadExceptions(string message)
            : base(message)
        {
            
        }
    }
}