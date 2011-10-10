using System.Text;

using Cassandra.Tests;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public abstract class CassandraFunctionalTestWithRemoveKeyspacesBase : CassandraFunctionalTestBase
    {
        #region Setup/Teardown

        public override void SetUp()
        {
            base.SetUp();
            
            cassandraClient = new CassandraClient(cassandraCluster);
            cassandraClient.RemoveAllKeyspaces();
            cassandraClient.AddKeyspace(Constants.KeyspaceName, Constants.ColumnFamilyName);
        }

        #endregion

        protected static Column ToColumn(string columnName, string columnValue, long? timestamp = null, int? ttl = null)
        {
            return new Column
                {
                    Name = columnName,
                    Value = ToBytes(columnValue),
                    Timestamp = timestamp,
                    TTL = ttl
                };
        }

        protected static byte[] ToBytes(string str)
        {
            return Encoding.UTF8.GetBytes(str);
        }

        protected static string ToString(byte[] bytes)
        {
            return Encoding.UTF8.GetString(bytes);
        }

        protected void Check(string key, string columnName, string columnValue, long? timestamp = null, int? ttl = null)
        {
            Column tryGetResult;
            Assert.IsTrue(cassandraClient.TryGetColumn(Constants.KeyspaceName, Constants.ColumnFamilyName, key, columnName, out tryGetResult));
            Column result = cassandraClient.GetColumn(Constants.KeyspaceName, Constants.ColumnFamilyName, key, columnName);
            tryGetResult.AssertEqualsTo(result);
            Assert.AreEqual(columnName, result.Name);
            Assert.AreEqual(columnValue, ToString(result.Value));
            if(timestamp == null)
                Assert.IsNotNull(result.Timestamp);
            else
                Assert.AreEqual(timestamp, result.Timestamp);
            Assert.AreEqual(ttl, result.TTL);
        }

        protected void CheckNotFound(string key, string columnName)
        {
            RunMethodWithException<ColumnIsNotFoundException>(
                () => cassandraClient.GetColumn(Constants.KeyspaceName, Constants.ColumnFamilyName, key, columnName));
            Column column;
            Assert.IsFalse(cassandraClient.TryGetColumn(Constants.KeyspaceName, Constants.ColumnFamilyName, key, columnName, out column));
        }

        protected ICassandraClient cassandraClient;
    }
}