using System;

namespace Cassandra.Tests.CoreTests.PoolTests
{
    public class AllItemsIsDeadExceptions : Exception
    {
        public AllItemsIsDeadExceptions(string message)
            : base(message)
        {
            
        }
    }
}