using System;
using System.Collections.Generic;
using System.Reflection;

using Cassandra.Tests;

using GroboContainer.Core;
using GroboContainer.Impl;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.FunctionalTests.Logger;
using SKBKontur.Cassandra.FunctionalTests.Utils;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public abstract class CassandraFunctionalTestBase : TestBase
    {
        public override void SetUp()
        {
            TestNameHolder.TestName = TestContext.CurrentContext.Test.Name;
            base.SetUp();
            KeyspaceName = "TestKeyspace_" + Guid.NewGuid().ToString("N");
            var assemblies = new List<Assembly>(AssembliesLoader.Load()) {Assembly.GetExecutingAssembly()};
            var container = new Container(new ContainerConfiguration(assemblies));
            cassandraCluster = container.Get<ICassandraCluster>();
            columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            ServiceUtils.ConfugureLog4Net(AppDomain.CurrentDomain.BaseDirectory);
            ClearKeyspacesOnce();
        }

        protected string KeyspaceName { get; private set; }

        protected ICassandraCluster cassandraCluster;
        protected IColumnFamilyConnection columnFamilyConnection;

        private void ClearKeyspacesOnce()
        {
            if(keyspacesDeleted) return;
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            var result = clusterConnection.RetrieveKeyspaces();
            foreach(var keyspace in result)
                cassandraCluster.RetrieveClusterConnection().RemoveKeyspace(keyspace.Name);
            keyspacesDeleted = true;
        }

        private static bool keyspacesDeleted;
    }
}