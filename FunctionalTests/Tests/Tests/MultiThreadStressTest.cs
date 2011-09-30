using System;
using System.Diagnostics;
using System.Text;
using System.Threading;

using CassandraClient.Abstractions;

using NUnit.Framework;

namespace Tests.Tests
{
    public class MultiThreadStressTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        [Test, Ignore]
        public void TestStress()
        {
            var random = new Random();

            var threads = new Thread[threadCount];

            for(int i = 0; i < threadCount; i++)
            {
                string key = RandomString(random, 20);
                threads[i] = new Thread(() => Test(key));
                threads[i].Start();
            }
            while(true)
            {
                int cnt = 0;
                for(int i = 0; i < threadCount; i++)
                {
                    if(!threads[i].IsAlive)
                        cnt++;
                }
                if(cnt == threadCount)
                    break;
            }
        }

        private void Test(string key)
        {
            var random = new Random();
            Log(key, "Fillind names...");
            var columnNames = new string[count];
            for(int i = 0; i < columnNames.Length; i++)
                columnNames[i] = RandomString(random, 20);

            Log(key, "Fillind values...");
            var columnValues = new byte[count][];
            for(int i = 0; i < columnValues.Length; i++)
            {
                columnValues[i] = new byte[columnSize];
                random.NextBytes(columnValues[i]);
            }

            Log(key, "Start writing...");
            for(int i = 0; i < columnValues.Length; i++)
            {
                if(i % 1000 == 0)
                    Log(key, "Writing " + i + " of " + columnValues.Length);
                cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, key, columnNames[i],
                                    columnValues[i]);
            }
            Log(key, "Start reading...");
            for(int i = 0; i < columnValues.Length; i++)
            {
                if(i % 1000 == 0)
                    Log(key, "Reading " + i + " of " + columnValues.Length);
                Column column;
                Assert.IsTrue(cassandraClient.TryGetColumn(Constants.KeyspaceName, Constants.ColumnFamilyName, key, columnNames[i], out column));
            }
        }

        private static string RandomString(Random rnd, int length)
        {
            var stringBuilder = new StringBuilder();
            for(int i = 0; i < length; i++)
                stringBuilder.Append('a' + rnd.Next(0, 26));
            return stringBuilder.ToString();
        }

        private void Log(string key, string str)
        {
            Debug.WriteLine(string.Format("key:{2} {0:yyyy-MM-ddTHH:mm:ss.fff}: {1}", DateTime.Now, str, key));
        }

        private const int columnSize = 1000;
        private const int count = 20000;
        private const int threadCount = 10;
    }
}