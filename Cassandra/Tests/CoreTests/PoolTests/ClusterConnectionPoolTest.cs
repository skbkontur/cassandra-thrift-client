using System;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;

using NUnit.Framework;

using Rhino.Mocks;

namespace Cassandra.Tests.CoreTests.PoolTests
{
    public class ClusterConnectionPoolTest: TestBase
    {
        private ClusterConnectionPool clusterPool;
        private Func<ConnectionPoolKey, IKeyspaceConnectionPool> func;
        private IKeyspaceConnectionPool keyspacePool2;
        private IKeyspaceConnectionPool keyspacePool1;
        private int count1;
        private int count2;
        private IPooledThriftConnection pooledThriftConnection;

        public override void SetUp()
        {
            base.SetUp();
            GetMock<ICassandraClusterSettings>();
            keyspacePool1 = GetMock<IKeyspaceConnectionPool>();
            keyspacePool2 = GetMock<IKeyspaceConnectionPool>();
            count1 = 0;
            count2 = 0;
            func = key =>
                       {
                           if (key.Keyspace == "key1")
                           {
                               count1++;
                               Assert.AreEqual(1, count1);
                               return keyspacePool1;
                           }
                           count2++;
                           Assert.AreEqual(1, count2);
                           return keyspacePool2;
                       };
            clusterPool = new ClusterConnectionPool(func);
            pooledThriftConnection = GetMock<IPooledThriftConnection>();
        }

        [Test]
        public void TryBorrowConnectionTest()
        {
            
            keyspacePool1.Expect(pool => pool.TryBorrowConnection(out ARG.Out(pooledThriftConnection).Dummy)).Return(ConnectionType.FromPool).Repeat.Times(10);
            for (int i = 0; i < 10; i++ )
                clusterPool.BorrowConnection(GetConnectionPoolKey("key1"));
            Assert.AreEqual(1, count1);
            Assert.AreEqual(0, count2);
            keyspacePool2.Expect(pool => pool.TryBorrowConnection(out ARG.Out(pooledThriftConnection).Dummy)).Return(ConnectionType.FromPool).Repeat.Times(10);
            for (int i = 0; i < 10; i++)
                clusterPool.BorrowConnection(GetConnectionPoolKey("key2"));
            Assert.AreEqual(1, count1);
            Assert.AreEqual(1, count2);
        }

        [Test]
        public void GetKnowledgesTest()
        {
            var knowledges = clusterPool.GetKnowledges();
            Assert.AreEqual(0, knowledges.Count);
            keyspacePool1.Expect(kp => kp.GetKnowledge()).Return(new KeyspaceConnectionPoolKnowledge {BusyConnectionCount = 146, FreeConnectionCount = 641});

            keyspacePool1.Expect(pool => pool.TryBorrowConnection(out ARG.Out(pooledThriftConnection).Dummy)).Return(ConnectionType.FromPool).Repeat.Times(10);
            for (int i = 0; i < 10; i++)
                clusterPool.BorrowConnection(GetConnectionPoolKey("key1"));
            knowledges = clusterPool.GetKnowledges();
            Assert.AreEqual(1, knowledges.Count);
            Assert.IsTrue(knowledges.ContainsKey(GetConnectionPoolKey("key1")));
            Assert.AreEqual(146, knowledges[GetConnectionPoolKey("key1")].BusyConnectionCount);
            Assert.AreEqual(641, knowledges[GetConnectionPoolKey("key1")].FreeConnectionCount);

            keyspacePool1.Expect(kp => kp.GetKnowledge()).Return(new KeyspaceConnectionPoolKnowledge { BusyConnectionCount = 146, FreeConnectionCount = 641 });
            keyspacePool2.Expect(kp => kp.GetKnowledge()).Return(new KeyspaceConnectionPoolKnowledge { BusyConnectionCount = 136, FreeConnectionCount = 631 });
            keyspacePool2.Expect(pool => pool.TryBorrowConnection(out ARG.Out(pooledThriftConnection).Dummy)).Return(ConnectionType.FromPool).Repeat.Times(10);
            for (int i = 0; i < 10; i++)
                clusterPool.BorrowConnection(GetConnectionPoolKey("key2"));
            knowledges = clusterPool.GetKnowledges();
            Assert.AreEqual(2, knowledges.Count);
            Assert.IsTrue(knowledges.ContainsKey(GetConnectionPoolKey("key1")));
            Assert.AreEqual(146, knowledges[GetConnectionPoolKey("key1")].BusyConnectionCount);
            Assert.AreEqual(641, knowledges[GetConnectionPoolKey("key1")].FreeConnectionCount);
            Assert.IsTrue(knowledges.ContainsKey(GetConnectionPoolKey("key2")));
            Assert.AreEqual(136, knowledges[GetConnectionPoolKey("key2")].BusyConnectionCount);
            Assert.AreEqual(631, knowledges[GetConnectionPoolKey("key2")].FreeConnectionCount);
        }

        private ConnectionPoolKey GetConnectionPoolKey(string keyspace)
        {
            return new ConnectionPoolKey
            {
                Keyspace = keyspace
            };
        }
    }
}