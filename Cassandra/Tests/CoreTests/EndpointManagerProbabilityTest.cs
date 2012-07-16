using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Core;

using NUnit.Framework;

namespace Cassandra.Tests.CoreTests
{
    public class EndpointManagerProbabilityTest: TestBase
    {
        private EndpointManager endpointManager;
        private Badlist badlist;

        public override void SetUp()
        {
            base.SetUp();
            badlist = new Badlist();
            endpointManager = new EndpointManager(badlist);
        }

        [Test]
        public void TopAttemtsToDeadTest()
        {
            var iter = 12000;
            var endPoint = new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 1}), 1);
            endpointManager.Register(endPoint);
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 2}), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 3}), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 4}), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 5}), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 6}), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 7}), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 8}), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 9}), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 10}), 1));
            for(int i = 0; i < iter; i++)
            {
                var currentEndpoint = endpointManager.GetEndPoints()[0];
                if (currentEndpoint.Equals(endPoint))
                {
                    endpointManager.Bad(currentEndpoint);
                } else
                {
                    endpointManager.Good(currentEndpoint);
                }
            }
            Assert.AreEqual(0.01, badlist.GetHealth(endPoint));
        }

        [Test]
        public void BottomAttemtsToDeadTest()
        {
            var iter = 500;
            var endPoint = new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 1 }), 1);
            endpointManager.Register(endPoint);
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 2 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 3 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 4 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 5 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 6 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 7 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 8 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 9 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 10 }), 1));
            for (int i = 0; i < iter; i++)
            {
                var currentEndpoint = endpointManager.GetEndPoints()[0];
                if (currentEndpoint.Equals(endPoint))
                {
                    endpointManager.Bad(currentEndpoint);
                }
                else
                {
                    endpointManager.Good(currentEndpoint);
                }
            }
            Assert.AreNotEqual(0.01, badlist.GetHealth(endPoint));
        }

        [Test]
        public void DeadNodeTest()
        {
            var iter = 100000;
            var endPoint = new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 1 }), 1);
            endpointManager.Register(endPoint);
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 2 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 3 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 4 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 5 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 6 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 7 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 8 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 9 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 10 }), 1));
            int counter = 0;
            for (int i = 0; i < 20; ++i)
            {
                endpointManager.Bad(endPoint);
            }
            for (int i = 0; i < iter; i++)
            {
                var currentEndpoint = endpointManager.GetEndPoints()[0];
                if (currentEndpoint.Equals(endPoint))
                {
                    counter++;
                }
            }
            Assert.Greater(30, Math.Abs(counter - 110));
        }

        [Test]
        public void TopAttemtsToUpTest()
        {
            var iter = 10000;
            var endPoint = new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 1 }), 1);
            endpointManager.Register(endPoint);
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 2 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 3 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 4 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 5 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 6 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 7 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 8 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 9 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 10 }), 1));
            for (int i = 0; i < 20; ++i)
            {
                endpointManager.Bad(endPoint);
            }
            for (int i = 0; i < iter; i++)
            {
                var currentEndpoint = endpointManager.GetEndPoints()[0];
                endpointManager.Good(currentEndpoint);
            }
            Assert.AreEqual(1.0, badlist.GetHealth(endPoint));
        }

        [Test]
        public void BottomAttemtsToUpTest()
        {
            var iter = 500;
            var endPoint = new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 1 }), 1);
            endpointManager.Register(endPoint);
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 2 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 3 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 4 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 5 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 6 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 7 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 8 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 9 }), 1));
            endpointManager.Register(new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 10 }), 1));
            for (int i = 0; i < 20; ++i)
            {
                endpointManager.Bad(endPoint);
            }
            for (int i = 0; i < iter; i++)
            {
                var currentEndpoint = endpointManager.GetEndPoints()[0];
                endpointManager.Good(currentEndpoint);
            }
            Assert.AreNotEqual(1.0, badlist.GetHealth(endPoint));
        }
    }
}