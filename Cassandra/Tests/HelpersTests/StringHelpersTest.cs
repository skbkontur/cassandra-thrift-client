using System.Text;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace Cassandra.Tests.HelpersTests
{
    public class StringHelpersTest : TestBase
    {
        [Test]
        public void TestToStringNull()
        {
            Assert.IsNull(StringExtensions.BytesToString(null));
        }

        [Test]
        public void TestToBytesNull()
        {
            Assert.IsNull(StringExtensions.StringToBytes(null));
        }

        [Test]
        public void TestToString()
        {
            var bytes = Encoding.UTF8.GetBytes("qxx");
            Assert.AreEqual("qxx", StringExtensions.BytesToString(bytes));
        }

        [Test]
        public void TestToBytes()
        {
            var expectedBytes = Encoding.UTF8.GetBytes("qxx");
            CollectionAssert.AreEqual(expectedBytes, StringExtensions.StringToBytes("qxx"));
        }
    }
}