using System;
using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions
{
    internal class AllItemsIsDeadExceptions : AggregateException
    {
        public AllItemsIsDeadExceptions(string message, IEnumerable<Exception> innerExceptions)
            : base(message, innerExceptions)
        {
        }

        public AllItemsIsDeadExceptions(string message)
            : base(message)
        {
        }
    }
}