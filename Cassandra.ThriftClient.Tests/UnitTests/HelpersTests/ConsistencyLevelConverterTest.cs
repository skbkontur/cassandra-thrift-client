using System;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

using ApacheConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SkbKontur.Cassandra.ThriftClient.Tests.UnitTests.HelpersTests
{
    public class ConsistencyLevelConverterTest : TestBase
    {
        [Test]
        public void TestConvert()
        {
            Assert.AreEqual(Enum.GetNames(typeof(ConsistencyLevel)).Length, 6);
            Assert.AreEqual(Enum.GetNames(typeof(ApacheConsistencyLevel)).Length, 11);
            DoTest(ConsistencyLevel.ALL, ApacheConsistencyLevel.ALL);
            DoTest(ConsistencyLevel.ANY, ApacheConsistencyLevel.ANY);
            DoTest(ConsistencyLevel.EACH_QUORUM, ApacheConsistencyLevel.EACH_QUORUM);
            DoTest(ConsistencyLevel.LOCAL_QUORUM, ApacheConsistencyLevel.LOCAL_QUORUM);
            DoTest(ConsistencyLevel.ONE, ApacheConsistencyLevel.ONE);
            DoTest(ConsistencyLevel.QUORUM, ApacheConsistencyLevel.QUORUM);
        }

        private static void DoTest(ConsistencyLevel consistencyLevel, ApacheConsistencyLevel expected)
        {
            Assert.AreEqual(expected, consistencyLevel.ToThriftConsistencyLevel());
        }
    }
}