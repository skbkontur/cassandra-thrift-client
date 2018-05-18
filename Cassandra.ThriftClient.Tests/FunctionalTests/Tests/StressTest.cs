using System;
using System.Diagnostics;
using System.Text;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class StressTest : CassandraFunctionalTestBase
    {
        [Test, Ignore("LongRunning")]
        public void TestStress()
        {
            const int count = 200000;

            var random = new Random();
            string key = RandomString(random, 30);

            Log("Fillind names...");
            var columnNames = new string[count];
            for(int i = 0; i < columnNames.Length; i++)
                columnNames[i] = RandomString(random, 20);

            Log("Fillind values...");
            var columnValues = new byte[count][];
            for(int i = 0; i < columnValues.Length; i++)
            {
                columnValues[i] = new byte[1000];
                random.NextBytes(columnValues[i]);
            }

            Log("Start writing...");
            for(int i = 0; i < columnValues.Length; i++)
            {
                if(i % 1000 == 0)
                    Log("Writing " + i + " of " + columnValues.Length);
                columnFamilyConnection.AddColumn(key, new Column
                    {
                        Name = columnNames[i],
                        Value = columnValues[i]
                   });
            }
            Log("Start reading...");
            for(int i = 0; i < columnValues.Length; i++)
            {
                if(i % 1000 == 0)
                    Log("Reading " + i + " of " + columnValues.Length);
                Column column;
                Assert.IsTrue(cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName)
                                  .TryGetColumn(key, columnNames[i], out column));
            }
        }

        private static string RandomString(Random rnd, int length)
        {
            var stringBuilder = new StringBuilder();
            for(int i = 0; i < length; i++)
                stringBuilder.Append('a' + rnd.Next(0, 26));
            return stringBuilder.ToString();
        }

        private void Log(string str)
        {
            Debug.WriteLine(string.Format("{0:yyyy-MM-ddTHH:mm:ss.fff}: {1}", DateTime.Now, str));
        }
    }
}