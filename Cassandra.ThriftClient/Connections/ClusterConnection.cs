using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Read;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Write;
using SKBKontur.Cassandra.CassandraClient.Core;

using Vostok.Logging;
using Vostok.Logging.Extensions;

using ApacheConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    internal class ClusterConnection : IClusterConnection
    {
        public ClusterConnection(ICommandExecutor<IFierceCommand> commandExecutor, ILog logger)
        {
            this.commandExecutor = commandExecutor;
            this.logger = logger;
        }

        public IList<Keyspace> RetrieveKeyspaces()
        {
            var retrieveKeyspacesCommand = new RetrieveKeyspacesCommand();
            commandExecutor.Execute(retrieveKeyspacesCommand);
            if(retrieveKeyspacesCommand.Keyspaces == null)
                return new List<Keyspace>();
            return retrieveKeyspacesCommand.Keyspaces;
        }

        public void UpdateKeyspace(Keyspace keyspace)
        {
            commandExecutor.Execute(new UpdateKeyspaceCommand(keyspace));
        }

        public void RemoveKeyspace(string keyspace)
        {
            commandExecutor.Execute(new DropKeyspaceCommand(keyspace));
        }

        public string DescribeVersion()
        {
            var describeVersionCommand = new DescribeVersionCommand();
            commandExecutor.Execute(describeVersionCommand);
            return describeVersionCommand.Version;
        }

        public void AddKeyspace(Keyspace keyspace)
        {
            var addKeyspaceCommand = new AddKeyspaceCommand(keyspace);
            commandExecutor.Execute(addKeyspaceCommand);
            logger.Info("Keyspace adding result: {0}", addKeyspaceCommand.Output);
        }

        public void WaitUntilSchemeAgreementIsReached(TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            do
            {
                var schemaAgreementCommand = new SchemaAgreementCommand();
                commandExecutor.Execute(schemaAgreementCommand);
                if(schemaAgreementCommand.Output.Count == 1)
                    return;
                LogVersions(schemaAgreementCommand.Output);
            } while (sw.Elapsed < timeout);
            throw new InvalidOperationException(string.Format("WaitUntilSchemeAgreementIsReached didn't complete in {0}", timeout));
        }

        private void LogVersions(IDictionary<string, List<string>> versions)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("Cassandra scheme is not synchronized:");
            foreach(var kvp in versions)
                stringBuilder.AppendLine(string.Format("\tVerson: {0}, Nodes: {1}", kvp.Key, string.Join(",", kvp.Value)));
            logger.Info(stringBuilder.ToString());
        }

        private readonly ICommandExecutor<IFierceCommand> commandExecutor;
        private readonly ILog logger;
    }
}