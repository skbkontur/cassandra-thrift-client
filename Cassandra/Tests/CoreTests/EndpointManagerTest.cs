using System.Collections.Generic;
using System.Net;

using CassandraClient.Core;

using NUnit.Framework;

using Rhino.Mocks;

namespace Cassandra.Tests.CoreTests
{
    public class EndpointManagerTest : TestBase
    {
        private IPEndPoint endpoint;
        private EndpointManager endpointManager;
        private IBadlist badlist;

        public override void SetUp()
        {
            base.SetUp();
            badlist = GetMock<IBadlist>();
            endpointManager = new EndpointManager(badlist);
            endpoint = new IPEndPoint(new IPAddress(new byte[] {1, 0, 0, 1}), 1212);
        }

        [Test]
        public void RegisterTest()
        {
            badlist.Expect(badlist1 => badlist1.Register(endpoint));
            endpointManager.Register(endpoint);
        }

        [Test]
        public void UnregisterTest()
        {
            badlist.Expect(badlist1 => badlist1.Unregister(endpoint));
            endpointManager.Unregister(endpoint);
        }

        [Test]
        public void GoodTest()
        {
            badlist.Expect(badlist1 => badlist1.Good(endpoint));
            badlist.Expect(badlist1 => badlist1.GetHealth(endpoint)).Return(1.0);
            endpointManager.Good(endpoint);
        }

        [Test]
        public void BadTest()
        {
            badlist.Expect(badlist1 => badlist1.Bad(endpoint));
            badlist.Expect(badlist1 => badlist1.GetHealth(endpoint)).Return(1.0);
            endpointManager.Bad(endpoint);
        }

        [Test]
        public void GetEndpointTest()
        {
            badlist.Expect(badlist1 => badlist1.GetHealthes()).Return(new[]
                {
                    new KeyValuePair<IPEndPoint, double>(endpoint, 1.0), 
                    new KeyValuePair<IPEndPoint, double>(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 1}), 2323), 0.0)
                });
            var currentEndpoint = endpointManager.GetEndPoint();
            Assert.AreEqual(endpoint, currentEndpoint);
        }
    }
}