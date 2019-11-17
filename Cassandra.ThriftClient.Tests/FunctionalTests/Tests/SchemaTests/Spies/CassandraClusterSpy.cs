using System;
using System.Collections.Generic;
using System.Linq;

using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Clusters.ActualizationEventListener;
using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.ThriftClient.Core.Pools;
using SkbKontur.Cassandra.ThriftClient.Scheme;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests.Spies
{
    public class CassandraClusterSpy : ICassandraCluster
    {
        public CassandraClusterSpy(Func<ICassandraCluster> cassandraClusterFactory)
        {
            innerCluster = cassandraClusterFactory();
        }

        public int UpdateColumnFamilyInvokeCount { get { return keyspaceConnectionSpies.Sum(x => x.UpdateColumnFamilyInvokeCount); } }

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

        public ITimeBasedColumnFamilyConnection RetrieveTimeBasedColumnFamilyConnection(string keyspace, string columnFamily)
        {
            return innerCluster.RetrieveTimeBasedColumnFamilyConnection(keyspace, columnFamily);
        }

        public IColumnFamilyConnectionImplementation RetrieveColumnFamilyConnectionImplementation(string keySpaceName, string columnFamilyName)
        {
            return innerCluster.RetrieveColumnFamilyConnectionImplementation(keySpaceName, columnFamilyName);
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            return innerCluster.GetKnowledges();
        }

        public void ActualizeKeyspaces(KeyspaceScheme[] keyspaces, ICassandraActualizerEventListener eventListener = null, bool changeExistingKeyspaceMetadata = false)
        {
            innerCluster.ActualizeKeyspaces(keyspaces, eventListener, changeExistingKeyspaceMetadata);
        }

        private readonly ICassandraCluster innerCluster;
        private readonly List<KeyspaceConnectionSpy> keyspaceConnectionSpies = new List<KeyspaceConnectionSpy>();
    }
}