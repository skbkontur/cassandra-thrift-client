using NUnit.Framework;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class ConnectionPropertiesTest : CassandraFunctionalTestBase
    {
        [Test]
        public void Test()
        {
            var parameters = columnFamilyConnection.GetConnectionParameters();
            Assert.AreEqual(5, parameters.Attempts);
            Assert.AreEqual(6000, parameters.Timeout);
        }
    }
}