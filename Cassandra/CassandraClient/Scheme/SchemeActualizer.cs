using System;
using System.Collections.Generic;
using System.Linq;

using log4net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Clusters.ActualizationEventListener;

namespace SKBKontur.Cassandra.CassandraClient.Scheme
{
    internal class SchemeActualizer
    {
        public SchemeActualizer(ICassandraCluster cassandraCluster, ICassandraActualizerEventListener eventListener)
        {
            this.cassandraCluster = cassandraCluster;
            this.eventListener = eventListener ?? EmptyCassandraActualizerEventListener.Instance;
            columnFamilyComparer = new ColumnFamilyEqualityByPropertiesComparer();
        }

        public void ActualizeKeyspaces(KeyspaceScheme[] keyspaceShemas)
        {
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            logger.Info("Start apply scheme...");
            eventListener.ActualizationStarted();
            if(keyspaceShemas == null || keyspaceShemas.Length == 0)
            {
                logger.Info("Found 0 keyspaces in scheme, stop applying scheme");
                return;
            }
            logger.InfoFormat("Found {0} keyspaces in scheme", keyspaceShemas.Length);
            var keyspaces = clusterConnection.RetrieveKeyspaces().ToDictionary(keyspace => keyspace.Name, StringComparer.OrdinalIgnoreCase);
            eventListener.SchemaRetrieved(keyspaces.Values.ToArray());
            logger.InfoFormat("Found {0} keyspaces in database", keyspaces.Count);
            foreach(var keyspaceScheme in keyspaceShemas)
            {
                logger.InfoFormat("Start actualize scheme for keyspace {0}", keyspaceScheme.Name);
                eventListener.KeyspaceActualizationStarted(keyspaceScheme.Name);
                var keyspace = new Keyspace
                    {
                        Name = keyspaceScheme.Name,
                        ReplicaPlacementStrategy = keyspaceScheme.Configuration.ReplicaPlacementStrategy.ToStringValue(),
                        ReplicationFactor = keyspaceScheme.Configuration.ReplicationFactor
                    };
                if(keyspaces.ContainsKey(keyspaceScheme.Name))
                {
                    logger.InfoFormat("Keyspace {0} already exists in the database, so run update keyspace command", keyspaceScheme.Name);
                    clusterConnection.UpdateKeyspace(keyspace);
                    ActualizeColumnFamilies(keyspaceScheme.Name, keyspaceScheme.Configuration.ColumnFamilies);
                }
                else
                {
                    logger.InfoFormat("Keyspace {0} is new, so run add keyspace command", keyspaceScheme.Name);
                    keyspace.ColumnFamilies = keyspaceScheme.Configuration.ColumnFamilies.ToDictionary(family => family.Name);
                    clusterConnection.AddKeyspace(keyspace);
                    eventListener.KeyspaceAdded(keyspace);
                }
            }
            eventListener.ActualizationCompleted();
        }

        private void ActualizeColumnFamilies(string keyspaceName, ColumnFamily[] columnFamilies)
        {
            logger.InfoFormat("Start actualize column families for keyspace '{0}'", keyspaceName);
            var keyspaceConnection = cassandraCluster.RetrieveKeyspaceConnection(keyspaceName);
            var keyspace = keyspaceConnection.DescribeKeyspace();
            var existsColumnFamilies = keyspace.ColumnFamilies ?? new Dictionary<string, ColumnFamily>();
            foreach(var columnFamily in columnFamilies)
            {
                logger.InfoFormat("Start actualize column family '{0}' for keyspace '{1}'", columnFamily.Name, keyspaceName);
                if(existsColumnFamilies.ContainsKey(columnFamily.Name))
                {
                    logger.InfoFormat("Column family '{0}' already exists in the keyspace '{1}', so run update column family command",
                                      columnFamily.Name, keyspaceName);
                    var existsColumnFamily = existsColumnFamilies[columnFamily.Name];
                    columnFamily.Id = existsColumnFamily.Id;
                    if(columnFamilyComparer.NeedUpdateColumnFamily(columnFamily, existsColumnFamily))
                    {
                        eventListener.ColumnFamilyUpdated(keyspaceName, columnFamily);
                        keyspaceConnection.UpdateColumnFamily(columnFamily);
                    }
                }
                else
                {
                    eventListener.ColumnFamilyAdded(keyspaceName, columnFamily);
                    keyspaceConnection.AddColumnFamily(columnFamily);
                }
            }
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraActualizerEventListener eventListener;

        private readonly ILog logger = LogManager.GetLogger(typeof(SchemeActualizer));
        private readonly ColumnFamilyEqualityByPropertiesComparer columnFamilyComparer;
    }
}