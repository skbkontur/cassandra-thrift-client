﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Read;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Write;
using SKBKontur.Cassandra.CassandraClient.Core;

using log4net;

using ApacheConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public class ClusterConnection : IClusterConnection
    {
        public ClusterConnection(ICommandExecuter commandExecuter)
        {
            this.commandExecuter = commandExecuter;
        }

        public IList<Keyspace> RetrieveKeyspaces()
        {
            var retrieveKeyspacesCommand = new RetrieveKeyspacesCommand();
            commandExecuter.Execute(retrieveKeyspacesCommand);
            if(retrieveKeyspacesCommand.Keyspaces == null)
                return new List<Keyspace>();
            return retrieveKeyspacesCommand.Keyspaces
                .Where(keyspace => !keyspace.Name.Equals("system", StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        public void UpdateKeyspace(Keyspace keyspace)
        {
            commandExecuter.Execute(new UpdateKeyspaceCommand(keyspace));
            WaitUntilAgreementIsReached();
        }

        public void RemoveKeyspace(string keyspace)
        {
            commandExecuter.Execute(new DropKeyspaceCommand(keyspace));
            WaitUntilAgreementIsReached();
        }

        public string DescribeVersion()
        {
            var describeVersionCommand = new DescribeVersionCommand();
            commandExecuter.Execute(describeVersionCommand);
            return describeVersionCommand.Version;
        }

        public void AddKeyspace(Keyspace keyspace)
        {
            commandExecuter.Execute(new AddKeyspaceCommand(keyspace));
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
                var schemaAgreementCommand = new SchemaAgreementCommand();
                commandExecuter.Execute(schemaAgreementCommand);

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

        private readonly ILog logger = LogManager.GetLogger(typeof(ClusterConnection));
    }
}