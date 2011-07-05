using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Aquiles;
using Aquiles.Command;
using Aquiles.Command.System;
using Aquiles.Model;

using CassandraClient.Abstractions;
using CassandraClient.Helpers;

using log4net;

namespace CassandraClient.Connections
{
    public class ClusterConnection : IClusterConnection
    {
        public ClusterConnection(IAquilesConnection aquilesConnection,
                                 ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel)
        {
            this.aquilesConnection = aquilesConnection;
            this.readConsistencyLevel = readConsistencyLevel.ToAquilesConsistencyLevel();
            this.writeConsistencyLevel = writeConsistencyLevel.ToAquilesConsistencyLevel();
        }

        public IList<Keyspace> RetrieveKeyspaces()
        {
            var retrieveKeyspacesCommand = new RetrieveKeyspacesCommand
                {
                    ConsistencyLevel = readConsistencyLevel
                };
            aquilesConnection.Execute(retrieveKeyspacesCommand);
            if(retrieveKeyspacesCommand.Keyspaces == null)
                return new List<Keyspace>();
            var result = new List<Keyspace>();
            // ReSharper disable LoopCanBeConvertedToQuery
            foreach(var aquilesKeyspace in retrieveKeyspacesCommand.Keyspaces)
                // ReSharper restore LoopCanBeConvertedToQuery
            {
                if(!aquilesKeyspace.Name.Equals("system", StringComparison.OrdinalIgnoreCase))
                    result.Add(aquilesKeyspace.ToKeyspace());
            }
            return result;
        }

        public void RemoveKeyspace(string keyspace)
        {
            aquilesConnection.Execute(new DropKeyspaceCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    Keyspace = keyspace
                });
            WaitUntilAgreementIsReached();
        }

        public void AddKeyspace(Keyspace keyspace)
        {
            aquilesConnection.Execute(new AddKeyspaceCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    KeyspaceDefinition = keyspace.ToAquilesKeyspace()
                });
            WaitUntilAgreementIsReached();
        }

        public void AddColumnFamily(string keyspace, string columnFamilyName)
        {
            aquilesConnection.Execute(new AddColumnFamilyCommand
                {
                    ColumnFamilyDefinition = new AquilesColumnFamily
                        {
                            Name = columnFamilyName,
                            Keyspace = keyspace
                        },
                    ConsistencyLevel = writeConsistencyLevel,
                });
            WaitUntilAgreementIsReached();
        }

        public Keyspace DescribeKeyspace(string keyspace)
        {
            var describeKeyspaceCommand = new DescribeKeyspaceCommand
                {
                    Keyspace = keyspace,
                    ConsistencyLevel = readConsistencyLevel
                };
            aquilesConnection.Execute(describeKeyspaceCommand);

            var keyspaceInformation = describeKeyspaceCommand.KeyspaceInformation;
            var result = new Keyspace
                {
                    ColumnFamilies =
                        keyspaceInformation.ColumnFamilies.ToDictionary(pair => pair.Key,
                                                                        pair => pair.Value.ToColumnFamily()),
                    Name = keyspaceInformation.Name,
                    ReplicaPlacementStrategy = keyspaceInformation.ReplicationPlacementStrategy,
                    ReplicationFactor = keyspaceInformation.ReplicationFactor
                };
            return result;
        }

        public void Dispose()
        {
            aquilesConnection.Dispose();
        }

        public void AddColumnFamily(ColumnFamily columnFamily)
        {
            aquilesConnection.Execute(new AddColumnFamilyCommand
                {
                    ColumnFamilyDefinition = columnFamily.ToAquilesColumnFamily(),
                    ConsistencyLevel = writeConsistencyLevel,
                });
            WaitUntilAgreementIsReached();
        }

        public void UpdateColumnFamily(ColumnFamily columnFamily)
        {
            var aquilesColumnFamily = columnFamily.ToAquilesColumnFamily();
            aquilesConnection.Execute(new UpdateColumnFamilyCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    ColumnFamilyDefinition = aquilesColumnFamily
                });
            WaitUntilAgreementIsReached();
        }

        public void RemoveColumnFamily(string columnFamily)
        {
            aquilesConnection.Execute(new DropColumnFamilyCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    ColumnFamily = columnFamily
                });
            WaitUntilAgreementIsReached();
        }

        private void WaitUntilAgreementIsReached()
        {
            while(true)
            {
                logger.Info("Start checking schema agreement.");
                var schemaAgreementCommand = new SchemaAgreementCommand
                    {
                        ConsistencyLevel = writeConsistencyLevel
                    };
                aquilesConnection.Execute(schemaAgreementCommand);

                if(schemaAgreementCommand.Output.Count == 1)
                    break;

                LogVersions(schemaAgreementCommand.Output);
                logger.Info("Finish checking schema agreement.");
            }
        }

        private void LogVersions(IDictionary<string, List<string>> versions)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("Agreement doesnt reach.");
            foreach(var agreeds in versions)
            {
                stringBuilder.AppendLine(string.Format("\tVerson: {0}, Nodes: {1}", agreeds.Key,
                                                       agreeds.Value.Aggregate("",
                                                                               (s1, s2) =>
                                                                               string.Format("{0}, {1}", s1, s2))));
            }
            logger.Info(stringBuilder.ToString());
        }

        private readonly IAquilesConnection aquilesConnection;
        private readonly ILog logger = LogManager.GetLogger("CassandraClientLogger");
        private readonly AquilesConsistencyLevel readConsistencyLevel;
        private readonly AquilesConsistencyLevel writeConsistencyLevel;
    }
}