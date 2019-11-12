using System;
using System.Diagnostics;
using System.Text;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

using SkbKontur.Cassandra.TimeBasedUuid;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class MultiThreadStressTest : CassandraFunctionalTestBase
    {
        [Test, Ignore("LongRunning")]
        public void TestStress()
        {
            var threads = new Thread[threadCount];
            threadStatuses = new int[threadCount];
            threadExceptions = new Exception[threadCount];

            for (var i = 0; i < threadCount; i++)
            {
                var key = RandomString(20);
                var threadIndex = i;
                threads[i] = new Thread(() => Test(threadIndex, key));
                threads[i].Start();
                threadStatuses[i] = 0;
            }
            while (true)
            {
                var cnt = 0;
                for (var i = 0; i < threadCount; i++)
                {
                    if (threadStatuses[i] == 1)
                        cnt++;
                }
                for (var i = 0; i < threadCount; i++)
                {
                    if (threadStatuses[i] == 2)
                        throw new Exception($"Поток {i} сдох", threadExceptions[i]);
                }
                if (cnt == threadCount)
                    break;
            }
        }

        private void Test(int threadIndex, string key)
        {
            try
            {
                Log(key, "Filling names...");
                var columnNames = new string[count];
                for (var i = 0; i < columnNames.Length; i++)
                    columnNames[i] = RandomString(20);

                Log(key, "Filling values...");
                var columnValues = new byte[count][];
                for (var i = 0; i < columnValues.Length; i++)
                {
                    columnValues[i] = new byte[columnSize];
                    ThreadLocalRandom.Instance.NextBytes(columnValues[i]);
                }

                Log(key, "Start writing...");
                for (var i = 0; i < columnValues.Length; i++)
                {
                    if (i % 1000 == 0)
                        Log(key, "Writing " + i + " of " + columnValues.Length);
                    columnFamilyConnection.AddColumn(key, new Column
                        {
                            Name = columnNames[i],
                            Value = columnValues[i]
                        });
                }
                Log(key, "Start reading...");
                for (var i = 0; i < columnValues.Length; i++)
                {
                    if (i % 1000 == 0)
                        Log(key, "Reading " + i + " of " + columnValues.Length);
                    Assert.IsTrue(cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName)
                                                  .TryGetColumn(key, columnNames[i], out var column));
                }
                threadStatuses[threadIndex] = 1;
            }
            catch (Exception e)
            {
                threadStatuses[threadIndex] = 2;
                threadExceptions[threadIndex] = e;
            }
        }

        private static string RandomString(int length)
        {
            var stringBuilder = new StringBuilder();
            for (var i = 0; i < length; i++)
                stringBuilder.Append('a' + ThreadLocalRandom.Instance.Next(0, 26));
            return stringBuilder.ToString();
        }

        private static void Log(string key, string str)
        {
            Debug.WriteLine("key:{2} {0:yyyy-MM-ddTHH:mm:ss.fff}: {1}", Timestamp.Now.ToDateTime(), str, key);
        }

        private const int columnSize = 1000;
        private const int count = 20000;
        private const int threadCount = 10;

        private volatile int[] threadStatuses;
        private volatile Exception[] threadExceptions;
    }
}