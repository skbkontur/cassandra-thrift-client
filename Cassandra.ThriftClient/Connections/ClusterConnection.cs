using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Commands.System.Read;
using SkbKontur.Cassandra.ThriftClient.Commands.System.Write;
using SkbKontur.Cassandra.ThriftClient.Core;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Connections
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
            if (retrieveKeyspacesCommand.Keyspaces == null)
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

        public void WaitUntilSchemaAgreementIsReached(TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            do
            {
                var schemaAgreementCommand = new SchemaAgreementCommand();
                commandExecutor.Execute(schemaAgreementCommand);
                if (schemaAgreementCommand.Output.Count == 1)
                    return;
                LogVersions(schemaAgreementCommand.Output);
            } while (sw.Elapsed < timeout);
            throw new InvalidOperationException($"WaitUntilSchemaAgreementIsReached didn't complete in {timeout}");
        }

        private void LogVersions(IDictionary<string, List<string>> versions)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("Cassandra schema is not synchronized:");
            foreach (var kvp in versions)
                stringBuilder.AppendLine($"\tVersion: {kvp.Key}, Nodes: {string.Join(",", kvp.Value)}");
            logger.Info(stringBuilder.ToString());
        }

        private readonly ICommandExecutor<IFierceCommand> commandExecutor;
        private readonly ILog logger;
    }
}