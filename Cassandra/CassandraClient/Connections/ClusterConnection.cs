using System;
using System.Collections.Generic;
using System.Text;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Helpers;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public class ClusterConnection : IClusterConnection
    {
        public ClusterConnection(ICommandExecuter commandExecuter,
                                 ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel, ICassandraLogManager logManager)
        {
            this.commandExecuter = commandExecuter;
            logger = logManager.GetLogger(GetType());
            this.readConsistencyLevel = readConsistencyLevel.ToAquilesConsistencyLevel();
            this.writeConsistencyLevel = writeConsistencyLevel.ToAquilesConsistencyLevel();
        }

        public IList<Keyspace> RetrieveKeyspaces()
        {
            var retrieveKeyspacesCommand = new RetrieveKeyspacesCommand
                {
                    ConsistencyLevel = readConsistencyLevel
                };
            commandExecuter.Execute(new AquilesCommandAdaptor(retrieveKeyspacesCommand));
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

        public void UpdateKeyspace(Keyspace keyspace)
        {
            commandExecuter.Execute(new AquilesCommandAdaptor(new UpdateKeyspaceCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    KeyspaceDefinition = keyspace.ToAquilesKeyspace()
                }));
            WaitUntilAgreementIsReached();
        }

        public void RemoveKeyspace(string keyspace)
        {
            commandExecuter.Execute(new AquilesCommandAdaptor(new DropKeyspaceCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    Keyspace = keyspace
                }));
            WaitUntilAgreementIsReached();
        }

        public string DescribeVersion()
        {
            var describeVersionCommand = new DescribeVersionCommand
                {
                    ConsistencyLevel = writeConsistencyLevel
                };
            commandExecuter.Execute(new AquilesCommandAdaptor(describeVersionCommand));
            return describeVersionCommand.Version;
        }

        public void AddKeyspace(Keyspace keyspace)
        {
            commandExecuter.Execute(new AquilesCommandAdaptor(new AddKeyspaceCommand
                {
                    ConsistencyLevel = writeConsistencyLevel,
                    KeyspaceDefinition = keyspace.ToAquilesKeyspace()
                }));
            WaitUntilAgreementIsReached();
        }

        public void Dispose()
        {
            commandExecuter.Dispose();
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
                commandExecuter.Execute(new AquilesCommandAdaptor(schemaAgreementCommand));

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
                stringBuilder.AppendLine(string.Format("\tVerson: {0}, Nodes: {1}", agreeds.Key, string.Join(",", agreeds.Value)));
            logger.Info(stringBuilder.ToString());
        }

        private readonly ICommandExecuter commandExecuter;
        private readonly ICassandraLogManager logManager;

        private readonly AquilesConsistencyLevel readConsistencyLevel;
        private readonly AquilesConsistencyLevel writeConsistencyLevel;
        private readonly ICassandraLogger logger;
    }
}