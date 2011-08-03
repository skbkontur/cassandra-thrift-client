using System;

using Aquiles.Command;

using CassandraClient.Abstractions;
using CassandraClient.Helpers;

using NUnit.Framework;

namespace Cassandra.Tests.CassandraClientTests.HelpersTests
{
    public class ConsistencyLevelConverterTest : TestBase
    {
        [Test]
        public void TestConvert()
        {
            Assert.AreEqual(Enum.GetNames(typeof(ConsistencyLevel)).Length, 6);
            Assert.AreEqual(Enum.GetNames(typeof(AquilesConsistencyLevel)).Length, 6);
            DoTest(ConsistencyLevel.ALL, AquilesConsistencyLevel.ALL);
            DoTest(ConsistencyLevel.ANY, AquilesConsistencyLevel.ANY);
            DoTest(ConsistencyLevel.EACH_QUORUM, AquilesConsistencyLevel.EACH_QUORUM);
            DoTest(ConsistencyLevel.LOCAL_QUORUM, AquilesConsistencyLevel.LOCAL_QUORUM);
            DoTest(ConsistencyLevel.ONE, AquilesConsistencyLevel.ONE);
            DoTest(ConsistencyLevel.QUORUM, AquilesConsistencyLevel.QUORUM);
        }

        private static void DoTest(ConsistencyLevel consistencyLevel, AquilesConsistencyLevel expected)
        {
            Assert.AreEqual(expected, consistencyLevel.ToAquilesConsistencyLevel());
        }
    }
}