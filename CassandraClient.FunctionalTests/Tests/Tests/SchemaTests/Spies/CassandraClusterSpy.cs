using System;
using System.Collections.Generic;
using System.Linq;

using GroboContainer.Infection;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Clusters.ActualizationEventListener;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests.Spies
{
    [IgnoredImplementation]
    public class CassandraClusterSpy : ICassandraCluster
    {
        public CassandraClusterSpy(Func<ICassandraCluster> cassandraClusterFactory)
        {
            innerCluster = cassandraClusterFactory();
        }

        public void Dispose()
        {
            innerCluster.Dispose();
        }

        public IClusterConnection RetrieveClusterConnection()
        {
            return innerCluster.RetrieveClusterConnection();
        }

        public IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName)
        {
            var result = new KeyspaceConnectionSpy(innerCluster.RetrieveKeyspaceConnection(keyspaceName));
            keyspaceConnectionSpies.Add(result);
            return result;
        }

        public IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName)
        {
            return innerCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
        }

        public IColumnFamilyConnectionImplementation<TColumn> GetColumnFamilyConnectionImplementation<TColumn>(string keySpaceName, string columnFamilyName) where TColumn : class, IColumn, new()
        {
            return innerCluster.GetColumnFamilyConnectionImplementation<TColumn>(keySpaceName, columnFamilyName);
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            return innerCluster.GetKnowledges();
        }

        public void ActualizeKeyspaces(KeyspaceScheme[] keyspaces, ICassandraActualizerEventListener eventListener = null)
        {
            innerCluster.ActualizeKeyspaces(keyspaces, eventListener);
        }

        public int UpdateColumnFamilyInvokeCount { get { return keyspaceConnectionSpies.Sum(x => x.UpdateColumnFamilyInvokeCount); } }

        private readonly List<KeyspaceConnectionSpy> keyspaceConnectionSpies = new List<KeyspaceConnectionSpy>();

        private readonly ICassandraCluster innerCluster;
    }
}