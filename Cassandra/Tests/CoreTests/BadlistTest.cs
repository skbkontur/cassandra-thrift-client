using System.Net;

using SKBKontur.Cassandra.CassandraClient.Core;

using NUnit.Framework;

namespace Cassandra.Tests.CoreTests
{
    public class BadlistTest : TestBase
    {
        private Badlist badlist;
        private IPEndPoint endpoint1;
        private IPEndPoint endpoint2;

        public override void SetUp()
        {
            base.SetUp();
            badlist = new Badlist();
            endpoint1 = new IPEndPoint(new IPAddress(new byte[] {12, 13, 14, 115}), 1212);
            endpoint2 = new IPEndPoint(new IPAddress(new byte[] {1, 13, 14, 115}), 1212);
            badlist.Register(endpoint1);
            badlist.Register(endpoint2);
        }

        [Test]
        public void TopGoodTest()
        {
            badlist.Good(endpoint1);
            Assert.AreEqual(1.0, badlist.GetHealth(endpoint1));
        }

        [Test]
        public void BadTest()
        {
            badlist.Bad(endpoint1);
            Assert.AreEqual(1.0 * 0.7, badlist.GetHealth(endpoint1));
        }

        [Test]
        public void MultiBadTest()
        {
            for (int i = 0; i < 100; ++i)
            {
                badlist.Bad(endpoint1);
            }
            Assert.AreEqual(0.01, badlist.GetHealth(endpoint1));
        }

        [Test]
        public void GoodBadTest()
        {
            for (int i = 0; i < 100; ++i)
            {
                badlist.Bad(endpoint1);
                badlist.Good(endpoint1);
            }
            Assert.AreEqual(1.0, badlist.GetHealth(endpoint1));
        }

        [Test]
        public void MultiEndpointBadTest()
        {
            badlist.Bad(endpoint1);
            badlist.Bad(endpoint2);
            badlist.Bad(endpoint1);
            Assert.AreEqual(0.7 * 0.7, badlist.GetHealth(endpoint1));
            Assert.AreEqual(0.7, badlist.GetHealth(endpoint2));
        }

        [Test]
        public void GetHealthesTest()
        {
            badlist.Bad(endpoint1);
            badlist.Bad(endpoint2);
            badlist.Bad(endpoint1);
            var healthes = badlist.GetHealthes();
            Assert.AreEqual(2, healthes.Length);
            Assert.AreEqual(healthes[0].Key.Equals(endpoint1) ? 0.7 * 0.7 : 0.7, healthes[0].Value);
            Assert.AreEqual(healthes[1].Key.Equals(endpoint1) ? 0.7 * 0.7 : 0.7, healthes[1].Value);
        }
    }
}