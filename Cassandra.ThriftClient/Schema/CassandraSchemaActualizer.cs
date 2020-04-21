using System;
using System.Collections.Generic;
using System.Linq;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Clusters.ActualizationEventListener;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Schema
{
    [PublicAPI]
    public class CassandraSchemaActualizer : ICassandraSchemaActualizer
    {
        public CassandraSchemaActualizer(ICassandraCluster cassandraCluster, ICassandraActualizerEventListener eventListener, ILog logger)
        {
            this.cassandraCluster = cassandraCluster;
            this.logger = logger.ForContext("CassandraThriftClient");
            this.eventListener = eventListener ?? EmptyCassandraActualizerEventListener.Instance;
        }

        public void ActualizeKeyspaces(KeyspaceSchema[] keyspaceShemas, bool changeExistingKeyspaceMetadata)
        {
            if (keyspaceShemas == null || keyspaceShemas.Length == 0)
            {
                logger.Info("Nothing to actualize since keyspaceShemas is empty");
                return;
            }

            logger.Info("Start schema actualization...");
            eventListener.ActualizationStarted();
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            clusterConnection.WaitUntilSchemaAgreementIsReached(schemaAgreementWaitTimeout);
            logger.Info("Found {0} keyspaces in schema", keyspaceShemas.Length);
            var keyspaces = clusterConnection.RetrieveKeyspaces().ToDictionary(keyspace => keyspace.Name, StringComparer.OrdinalIgnoreCase);
            eventListener.SchemaRetrieved(keyspaces.Values.ToArray());
            logger.Info("Found {0} keyspaces in database", keyspaces.Count);
            foreach (var keyspaceSchema in keyspaceShemas)
            {
                logger.Info("Start schema actualization for keyspace: {0}", keyspaceSchema.Name);
                eventListener.KeyspaceActualizationStarted(keyspaceSchema.Name);
                var keyspace = new Keyspace
                    {
                        Name = keyspaceSchema.Name,
                        DurableWrites = keyspaceSchema.Configuration.DurableWrites,
                        ReplicationStrategy = keyspaceSchema.Configuration.ReplicationStrategy,
                    };
                if (keyspaces.ContainsKey(keyspaceSchema.Name))
                {
                    if (changeExistingKeyspaceMetadata)
                    {
                        logger.Info("Keyspace {0} already exists in the database, so run update keyspace command", keyspaceSchema.Name);
                        clusterConnection.UpdateKeyspace(keyspace);
                    }
                    else
                        logger.Info("Keyspace {0} already exists in the database, changeExistingKeyspaceMetadata is set to False, so do not run update keyspace command", keyspaceSchema.Name);
                    ActualizeColumnFamilies(keyspaceSchema.Name, keyspaceSchema.Configuration.ColumnFamilies);
                }
                else
                {
                    logger.Info("Keyspace {0} is new, so run add keyspace command", keyspaceSchema.Name);
                    keyspace.ColumnFamilies = keyspaceSchema.Configuration.ColumnFamilies.ToDictionary(family => family.Name);
                    clusterConnection.AddKeyspace(keyspace);
                    eventListener.KeyspaceAdded(keyspace);
                }
            }
            clusterConnection.WaitUntilSchemaAgreementIsReached(schemaAgreementWaitTimeout);
            eventListener.ActualizationCompleted();
        }

        private void ActualizeColumnFamilies(string keyspaceName, ColumnFamily[] columnFamilies)
        {
            logger.Info("Start actualize column families for keyspace '{0}'", keyspaceName);
            var keyspaceConnection = cassandraCluster.RetrieveKeyspaceConnection(keyspaceName);
            var keyspace = keyspaceConnection.DescribeKeyspace();
            var existsColumnFamilies = keyspace.ColumnFamilies ?? new Dictionary<string, ColumnFamily>();
            foreach (var columnFamily in columnFamilies)
            {
                logger.Info("Start actualize column family '{0}' for keyspace '{1}'", columnFamily.Name, keyspaceName);
                if (existsColumnFamilies.ContainsKey(columnFamily.Name))
                {
                    var existsColumnFamily = existsColumnFamilies[columnFamily.Name];
                    columnFamily.Id = existsColumnFamily.Id;
                    if (columnFamilyComparer.NeedUpdateColumnFamily(columnFamily, existsColumnFamily))
                    {
                        logger.Info("Column family '{0}' already exists in the keyspace '{1}' and needs to be altered, so run UpdateColumnFamily command", columnFamily.Name, keyspaceName);
                        eventListener.ColumnFamilyUpdated(keyspaceName, columnFamily);
                        keyspaceConnection.UpdateColumnFamily(columnFamily);
                    }
                }
                else
                {
                    logger.Info("Column family '{0}' does not exist in the keyspace '{1}', so run AddColumnFamily command", columnFamily.Name, keyspaceName);
                    eventListener.ColumnFamilyAdded(keyspaceName, columnFamily);
                    keyspaceConnection.AddColumnFamily(columnFamily);
                }
            }
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ILog logger;
        private readonly ICassandraActualizerEventListener eventListener;
        private readonly ColumnFamilyEqualityByPropertiesComparer columnFamilyComparer = new ColumnFamilyEqualityByPropertiesComparer();
        private readonly TimeSpan schemaAgreementWaitTimeout = TimeSpan.FromMinutes(1);
    }
}