using System;
using System.Linq;
using System.Threading;

using Cassandra.ThriftClient.Tests.FunctionalTests.Utils;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Connections;

using SkbKontur.Cassandra.TimeBasedUuid;

using Vostok.Logging.Abstractions;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class ReadWriteTest : CassandraFunctionalTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            connection = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
        }

        [Test]
        public void TestReadWrite()
        {
            threads = new Thread[threadsCount];
            for (int i = 0; i < threadsCount; i++)
                threads[i] = new Thread(ThreadAction);
            for (int i = 0; i < threadsCount; i++)
                threads[i].Start();
            const int minutesCount = 1;
            for (int i = 0; i < minutesCount * 12; i++)
            {
                Thread.Sleep(5000);
                for (int j = 0; j < threadsCount; j++)
                    Assert.That(threads[j].IsAlive);
            }
        }

        public override void TearDown()
        {
            stop = true;
            for (int i = 0; i < threadsCount; i++)
                threads[i].Join();
            base.TearDown();
        }

        public void ThreadAction()
        {
            Logger.Instance.Info("Start ThreadAction");
            while (true)
            {
                if (stop) 
                    return;
                try
                {
                    var guid = Guid.NewGuid().ToString();
                    var row = $"row{ThreadLocalRandom.Instance.Next(10)}";
                    Add(row, guid);
                    if (!CheckIn(row, guid))
                        throw new Exception("bug");
                    Delete(row, guid);
                }
                catch (Exception e)
                {
                    Logger.Instance.Error(e);
                    throw;
                }
            }
        }

        private void Add(string row, string id)
        {
            connection.AddColumn(row, new Column
                {
                    Name = id,
                    Timestamp = 0,
                    Value = new byte[] {0}
                });
        }

        private void Delete(string row, string id)
        {
            connection.DeleteBatch(row, new[] {id}, 1);
        }

        private bool CheckIn(string row, string id)
        {
            var ids = connection.GetRow(row).Select(t => t.Name).ToArray();
            var res = ids.Any(t => t == id);
            if (!res)
                Logger.Instance.Info("Was [" + row + "]:\n" + string.Join(",\n", ids) + "\nNeeded:\n" + id + "\n");
            return res;
        }

        private const int threadsCount = 30;

        private volatile bool stop;
        private Thread[] threads;
        private IColumnFamilyConnection connection;
    }
}