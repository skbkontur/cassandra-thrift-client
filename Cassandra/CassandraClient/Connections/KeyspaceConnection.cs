using System.Collections.Generic;
using System.Linq;
using System.Text;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Helpers;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public class KeyspaceConnection : IKeyspaceConnection
    {
        public KeyspaceConnection(ICommandExecuter commandExecuter,
                                  ConsistencyLevel readConsistencyLevel,
                                  ConsistencyLevel writeConsistencyLevel,
                                  string keyspace)
        {
            this.writeConsistencyLevel = writeConsistencyLevel.ToAquilesConsistencyLevel();
            this.commandExecuter = commandExecuter;
            this.keyspace = keyspace;
            this.readConsistencyLevel = readConsistencyLevel.ToAquilesConsistencyLevel();
        }

        public void AddColumnFamily(ColumnFamily columnFamily)
        {
            commandExecuter.Execute(new AquilesCommandAdaptor(new AddColumnFamilyCommand
                {
                    ColumnFamilyDefinition = columnFamily.ToAquilesColumnFamily(keyspace),
                    ConsistencyLevel = writeConsistencyLevel,
                }, keyspace));
            WaitUntilAgreementIsReached();
        }

        public void AddColumnFamily(string columnFamilyName)
        {
            commandExecuter.Execute(new AquilesCommandAdaptor(new AddColumnFamilyCommand
                {
                    ColumnFamilyDefinition = new AquilesColumnFamily
                        {
                            Name = columnFamilyName,
                            Keyspace = keyspace
                        },
                    ConsistencyLevel = writeConsistencyLevel,
                }, keyspace));
            WaitUntilAgreementIsReached();
        }

        public Keyspace DescribeKeyspace()
        {
            var describeKeyspaceCommand = new DescribeKeyspaceCommand
                {
                    Keyspace = keyspace,
                    ConsistencyLevel = readConsistencyLevel
                };
            commandExecuter.Execute(new AquilesCommandAdaptor(describeKeyspaceCommand, keyspace));

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

        public void UpdateColumnFamily(ColumnFamily columnFamily)
        {
            AquilesColumnFamily aquilesColumnFamily = columnFamily.ToAquilesColumnFamily(keyspace);
            commandExecuter.Execute(new AquilesCommandAdaptor(new UpdateColumnFamilyCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    ColumnFamilyDefinition = aquilesColumnFamily
                }, keyspace));
            WaitUntilAgreementIsReached();
        }

        public void RemoveColumnFamily(string columnFamily)
        {
            commandExecuter.Execute(new AquilesCommandAdaptor(new DropColumnFamilyCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    ColumnFamily = columnFamily
                }, keyspace));
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
                commandExecuter.Execute(new AquilesCommandAdaptor(schemaAgreementCommand, keyspace));

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

        private readonly ICommandExecuter commandExecuter;
        private readonly string keyspace;

        private readonly AquilesConsistencyLevel readConsistencyLevel;
        private readonly AquilesConsistencyLevel writeConsistencyLevel;
        private readonly ILog logger = LogManager.GetLogger(typeof(KeyspaceConnection));
    }
}