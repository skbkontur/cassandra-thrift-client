using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class ConnectionPoolTest : CassandraFunctionalTestBase
    {
        [Test]
        public void TestSimple()
        {
            DoTest(10);
        }

        [Test]
        public void TestHard()
        {
            DoTest(100);
        }

        private void DoTest(int threadCount)
        {
            threads = new List<Thread>();
            finished = new int[threadCount];
            stopped = false;
            for (var i = 0; i < threadCount; i++)
            {
                var i1 = i;
                var thread = new Thread(() => FillColumnFamily(i1));
                threads.Add(thread);
                thread.Start();
            }
            var maxFree = 0;
            var maxBusy = 0;
            while (true)
            {
                if (stopped)
                    break;
                Thread.Sleep(5000);
                var know = cassandraCluster.GetKnowledges();
                Console.WriteLine("-------------------------------");
                Console.WriteLine(know.Count);
                foreach (var kvp in know)
                {
                    Console.WriteLine(kvp.Key.IpEndPoint + " " + kvp.Key.Keyspace + " " + kvp.Value.BusyConnectionCount + " " + kvp.Value.FreeConnectionCount);
                    maxBusy = Math.Max(maxBusy, kvp.Value.BusyConnectionCount);
                    maxFree = Math.Max(maxFree, kvp.Value.FreeConnectionCount);
                    Assert.IsTrue(kvp.Value.BusyConnectionCount < 3 * threadCount);
                    Assert.IsTrue(kvp.Value.FreeConnectionCount < 3 * threadCount);
                }

                var flag = threads.Aggregate(false, (current, thread) => current || (thread.IsAlive));
                if (!flag || stopped) break;
                for (var i = 0; i < threadCount; i++)
                {
                    if (finished[i] == -1)
                        stopped = true;
                }
            }
            for (var i = 0; i < threadCount; i++)
                threads[i].Join();

            for (var i = 0; i < threadCount; i++)
                Assert.AreEqual(1, finished[i]);
            Console.WriteLine("Max free = {0}; Max busy: {1}", maxFree, maxBusy);
        }

        private void FillColumnFamily(int id)
        {
            try
            {
                for (var i = 0; i < 100; i++)
                {
                    if (stopped)
                        break;
                    var connection = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);

                    var list = new List<Column>();
                    for (var j = 0; j < 1000; j++)
                    {
                        if (stopped)
                            break;
                        list.Add(new Column
                            {
                                Name = $"name_{id}_{i}_{j}",
                                Value = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}
                            });
                    }
                    if (stopped)
                        break;
                    connection.AddBatch($"row_{id}_{i}", list);
                }
            }
            catch
            {
                finished[id] = -1;
                throw;
            }
            finished[id] = 1;
        }

        public volatile int[] finished;

        private List<Thread> threads;
        private volatile bool stopped;
    }
}