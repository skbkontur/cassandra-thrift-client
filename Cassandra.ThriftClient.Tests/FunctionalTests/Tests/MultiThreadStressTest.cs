using System;
using System.Diagnostics;
using System.Text;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class MultiThreadStressTest : CassandraFunctionalTestBase
    {
        [Test, Ignore("LongRunning")]
        public void TestStress()
        {
            var random = new Random();

            var threads = new Thread[threadCount];
            threadStatuses = new int[threadCount];
            threadExceptions = new Exception[threadCount];

            for (int i = 0; i < threadCount; i++)
            {
                string key = RandomString(random, 20);
                int threadIndex = i;
                threads[i] = new Thread(() => Test(threadIndex, key));
                threads[i].Start();
                threadStatuses[i] = 0;
            }
            while (true)
            {
                int cnt = 0;
                for (int i = 0; i < threadCount; i++)
                {
                    if (threadStatuses[i] == 1)
                        cnt++;
                }
                for (int i = 0; i < threadCount; i++)
                {
                    if (threadStatuses[i] == 2)
                        throw new Exception(string.Format("Поток {0} сдох", i), threadExceptions[i]);
                }
                if (cnt == threadCount)
                    break;
            }
        }

        private void Test(int threadIndex, string key)
        {
            try
            {
                var random = new Random();
                Log(key, "Fillind names...");
                var columnNames = new string[count];
                for (int i = 0; i < columnNames.Length; i++)
                    columnNames[i] = RandomString(random, 20);

                Log(key, "Fillind values...");
                var columnValues = new byte[count][];
                for (int i = 0; i < columnValues.Length; i++)
                {
                    columnValues[i] = new byte[columnSize];
                    random.NextBytes(columnValues[i]);
                }

                Log(key, "Start writing...");
                for (int i = 0; i < columnValues.Length; i++)
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
                for (int i = 0; i < columnValues.Length; i++)
                {
                    if (i % 1000 == 0)
                        Log(key, "Reading " + i + " of " + columnValues.Length);
                    Column column;
                    Assert.IsTrue(cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName)
                                                  .TryGetColumn(key, columnNames[i], out column));
                }
                threadStatuses[threadIndex] = 1;
            }
            catch (Exception e)
            {
                threadStatuses[threadIndex] = 2;
                threadExceptions[threadIndex] = e;
            }
        }

        private static string RandomString(Random rnd, int length)
        {
            var stringBuilder = new StringBuilder();
            for (int i = 0; i < length; i++)
                stringBuilder.Append('a' + rnd.Next(0, 26));
            return stringBuilder.ToString();
        }

        private void Log(string key, string str)
        {
            Debug.WriteLine("key:{2} {0:yyyy-MM-ddTHH:mm:ss.fff}: {1}", DateTime.Now, str, key);
        }

        private const int columnSize = 1000;
        private const int count = 20000;
        private const int threadCount = 10;

        private volatile int[] threadStatuses;
        private volatile Exception[] threadExceptions;
    }
}