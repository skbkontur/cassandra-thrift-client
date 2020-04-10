using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Clusters.ActualizationEventListener;
using SkbKontur.Cassandra.ThriftClient.Exceptions;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Scheme
{
    internal class SchemeActualizer
    {
        public SchemeActualizer(ICassandraCluster cassandraCluster, ICassandraActualizerEventListener eventListener, ILog logger)
        {
            this.cassandraCluster = cassandraCluster;
            this.logger = logger;
            this.eventListener = eventListener ?? EmptyCassandraActualizerEventListener.Instance;
            columnFamilyComparer = new ColumnFamilyEqualityByPropertiesComparer();
        }

        public void ActualizeKeyspaces(KeyspaceScheme[] keyspaceShemas, bool changeExistingKeyspaceMetadata, TimeSpan? timeout = null)
        {
            if (keyspaceShemas == null || keyspaceShemas.Length == 0)
            {
                logger.Info("Found 0 keyspaces in scheme, skip applying scheme");
                return;
            }
            var sw = Stopwatch.StartNew();
            timeout = timeout ?? TimeSpan.FromMinutes(5);
            do
            {
                try
                {
                    DoActualizeKeyspaces(keyspaceShemas, changeExistingKeyspaceMetadata);
                    return;
                }
                catch (CassandraClientIOException e)
                {
                    logger.Warn("CassandraClientIOException (e.g. socket timeout) occured during scheme actualization", e);
                }
                catch (CassandraClientTimedOutException e)
                {
                    logger.Warn("CassandraClientTimedOutException occured during scheme actualization", e);
                }
            } while (sw.Elapsed < timeout);
            throw new InvalidOperationException($"Failed to actualize cassandra scheme in {timeout}");
        }

        private void DoActualizeKeyspaces(KeyspaceScheme[] keyspaceShemas, bool changeExistingKeyspaceMetadata)
        {
            logger.Info("Start apply scheme...");
            eventListener.ActualizationStarted();
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            clusterConnection.WaitUntilSchemeAgreementIsReached(TimeSpan.FromMinutes(1));
            logger.Info("Found {0} keyspaces in scheme", keyspaceShemas.Length);
            var keyspaces = clusterConnection.RetrieveKeyspaces().ToDictionary(keyspace => keyspace.Name, StringComparer.OrdinalIgnoreCase);
            eventListener.SchemaRetrieved(keyspaces.Values.ToArray());
            logger.Info("Found {0} keyspaces in database", keyspaces.Count);
            foreach (var keyspaceScheme in keyspaceShemas)
            {
                logger.Info("Start actualize scheme for keyspace {0}", keyspaceScheme.Name);
                eventListener.KeyspaceActualizationStarted(keyspaceScheme.Name);
                var keyspace = new Keyspace
                    {
                        Name = keyspaceScheme.Name,
                        DurableWrites = keyspaceScheme.Configuration.DurableWrites,
                        ReplicationStrategy = keyspaceScheme.Configuration.ReplicationStrategy,
                    };
                if (keyspaces.ContainsKey(keyspaceScheme.Name))
                {
                    if (changeExistingKeyspaceMetadata)
                    {
                        logger.Info("Keyspace {0} already exists in the database, so run update keyspace command", keyspaceScheme.Name);
                        clusterConnection.UpdateKeyspace(keyspace);
                    }
                    else
                        logger.Info("Keyspace {0} already exists in the database, changeExistingKeyspaceMetadata is set to False, so do not run update keyspace command", keyspaceScheme.Name);
                    ActualizeColumnFamilies(keyspaceScheme.Name, keyspaceScheme.Configuration.ColumnFamilies);
                }
                else
                {
                    logger.Info("Keyspace {0} is new, so run add keyspace command", keyspaceScheme.Name);
                    keyspace.ColumnFamilies = keyspaceScheme.Configuration.ColumnFamilies.ToDictionary(family => family.Name);
                    clusterConnection.AddKeyspace(keyspace);
                    eventListener.KeyspaceAdded(keyspace);
                }
            }
            clusterConnection.WaitUntilSchemeAgreementIsReached(TimeSpan.FromMinutes(1));
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
        private readonly ColumnFamilyEqualityByPropertiesComparer columnFamilyComparer;
        private readonly ICassandraActualizerEventListener eventListener;
    }
}