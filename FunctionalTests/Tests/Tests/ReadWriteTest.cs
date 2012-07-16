using System;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Connections;

using log4net;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class ReadWriteTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        public override void SetUp()
        {
            base.SetUp();
            connection = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName);
            checkConnectionsThread = new Thread(CheckConnections);
            checkConnectionsThread.Start();
        }

        [Test]
        public void TestReadWrite()
        {
            threads = new Thread[threadsCount];
            for(int i = 0; i < threadsCount; i++)
                threads[i] = new Thread(ThreadAction);
            for(int i = 0; i < threadsCount; i++)
                threads[i].Start();
            const int minutesCount = 1;
            for(int i = 0; i < minutesCount * 12; i++)
            {
                Thread.Sleep(5000);
                for(int j = 0; j < threadsCount; j++)
                    Assert.That(threads[j].IsAlive);
            }
        }

        public override void TearDown()
        {
            stop = true;
            for(int i = 0; i < threadsCount; i++)
                threads[i].Join();
            checkConnectionsThread.Join();
            base.TearDown();
        }

        public void ThreadAction()
        {
            logger.Info("Start ThreadAction");
            var random = new Random(Guid.NewGuid().GetHashCode());
            while(true)
            {
                if(stop) return;
                try
                {
                    var guid = Guid.NewGuid().ToString();
                    var row = "row" + random.Next(10);
                    Add(row, guid);
                    if(!CheckIn(row, guid))
                        throw new Exception("bug");
                    Delete(row, guid);
                }
                catch(Exception e)
                {
                    logger.Error(e);
                    throw;
                }
            }
        }

        private void CheckConnections()
        {
            while(true)
            {
                if(stop) return;
                cassandraCluster.CheckConnections();
                Thread.Sleep(100);
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
            if(!res)
                logger.Info("Was [" + row + "]:\n" + string.Join(",\n", ids) + "\nNeeded:\n" + id + "\n");
            return res;
        }

        private volatile bool stop;
        private readonly ILog logger = LogManager.GetLogger(typeof(ReadWriteTest));
        private Thread[] threads;
        private IColumnFamilyConnection connection;
        private Thread checkConnectionsThread;
        private const int threadsCount = 30;
    }
}