using System.Diagnostics;
using System.Text;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

using SkbKontur.Cassandra.TimeBasedUuid;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class StressTest : CassandraFunctionalTestBase
    {
        [Test, Ignore("LongRunning")]
        public void TestStress()
        {
            const int count = 200000;

            string key = RandomString(30);

            Log("Filling names...");
            var columnNames = new string[count];
            for (int i = 0; i < columnNames.Length; i++)
                columnNames[i] = RandomString(20);

            Log("Filling values...");
            var columnValues = new byte[count][];
            for (int i = 0; i < columnValues.Length; i++)
            {
                columnValues[i] = new byte[1000];
                ThreadLocalRandom.Instance.NextBytes(columnValues[i]);
            }

            Log("Start writing...");
            for (int i = 0; i < columnValues.Length; i++)
            {
                if (i % 1000 == 0)
                    Log("Writing " + i + " of " + columnValues.Length);
                columnFamilyConnection.AddColumn(key, new Column
                    {
                        Name = columnNames[i],
                        Value = columnValues[i]
                    });
            }
            Log("Start reading...");
            for (int i = 0; i < columnValues.Length; i++)
            {
                if (i % 1000 == 0)
                    Log("Reading " + i + " of " + columnValues.Length);
                Assert.IsTrue(cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName)
                                              .TryGetColumn(key, columnNames[i], out var column));
            }
        }

        private static string RandomString(int length)
        {
            var stringBuilder = new StringBuilder();
            for (int i = 0; i < length; i++)
                stringBuilder.Append('a' + ThreadLocalRandom.Instance.Next(0, 26));
            return stringBuilder.ToString();
        }

        private static void Log(string str)
        {
            Debug.WriteLine("{0:yyyy-MM-ddTHH:mm:ss.fff}: {1}", Timestamp.Now.ToDateTime(), str);
        }
    }
}