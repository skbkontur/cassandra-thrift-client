using System;
using System.Collections.Generic;

namespace SkbKontur.Cassandra.ThriftClient.Core.GenericPool.Exceptions
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