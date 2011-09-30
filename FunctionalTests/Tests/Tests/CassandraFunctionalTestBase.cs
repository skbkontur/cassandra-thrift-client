using System;
using System.Collections.Generic;
using System.Reflection;

using Cassandra.Tests;

using CassandraClient.Clusters;

using GroboContainer.Core;
using GroboContainer.Impl;

using Tests.Utils;

namespace Tests.Tests
{
    public abstract class CassandraFunctionalTestBase : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            var assemblies = new List<Assembly>(AssembliesLoader.Load()) {Assembly.GetExecutingAssembly()};
            var container = new Container(new ContainerConfiguration(assemblies));
            cassandraCluster = container.Get<ICassandraCluster>();
            ServiceUtils.ConfugureLog4Net(AppDomain.CurrentDomain.BaseDirectory);
        }

        protected ICassandraCluster cassandraCluster;
    }
}