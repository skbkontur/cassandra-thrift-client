using System.Text;

using CassandraClient.Helpers;

using NUnit.Framework;

namespace Cassandra.Tests.HelpersTests
{
    public class StringHelpersTest : TestBase
    {
        [Test]
        public void TestToStringNull()
        {
            Assert.IsNull(StringHelpers.BytesToString(null));
        }

        [Test]
        public void TestToBytesNull()
        {
            Assert.IsNull(StringHelpers.StringToBytes(null));
        }

        [Test]
        public void TestToString()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("qxx");
            Assert.AreEqual("qxx", StringHelpers.BytesToString(bytes));
        }

        [Test]
        public void TestToBytes()
        {
            byte[] expectedBytes = Encoding.UTF8.GetBytes("qxx");
            CollectionAssert.AreEqual(expectedBytes, StringHelpers.StringToBytes("qxx"));
        }
    }
}