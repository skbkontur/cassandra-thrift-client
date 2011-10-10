using System;
using System.Collections.Generic;
using System.Reflection;

using Cassandra.Tests;
using Cassandra.Tests.ConsoleLog;

using GroboContainer.Core;
using GroboContainer.Impl;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Log;
using SKBKontur.Cassandra.FunctionalTests.Logger;
using SKBKontur.Cassandra.FunctionalTests.Utils;


namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public abstract class CassandraFunctionalTestBase : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            var assemblies = new List<Assembly>(AssembliesLoader.Load()) {Assembly.GetExecutingAssembly()};
            var container = new Container(new ContainerConfiguration(assemblies));
            container.Configurator.ForAbstraction<ICassandraLogManager>().UseInstances(new TestLogManager());

            cassandraCluster = container.Get<ICassandraCluster>();
            ServiceUtils.ConfugureLog4Net(AppDomain.CurrentDomain.BaseDirectory);
            TestNameHolder.TestName = TestContext.CurrentContext.Test.Name;
        }

        protected ICassandraCluster cassandraCluster;
    }
}