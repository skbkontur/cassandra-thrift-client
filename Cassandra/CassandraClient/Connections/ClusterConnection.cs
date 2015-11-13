using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

using log4net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Read;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Write;
using SKBKontur.Cassandra.CassandraClient.Core;

using ApacheConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    internal class ClusterConnection : IClusterConnection
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
                                           .Where(keyspace => !IsSystemKeyspace(keyspace.Name))
                                           .ToList();
        }

        public void UpdateKeyspace(Keyspace keyspace)
        {
            commandExecuter.ExecuteSchemeUpdateCommandOnce(new UpdateKeyspaceCommand(keyspace));
        }

        public void RemoveKeyspace(string keyspace)
        {
            commandExecuter.ExecuteSchemeUpdateCommandOnce(new DropKeyspaceCommand(keyspace));
        }

        public string DescribeVersion()
        {
            var describeVersionCommand = new DescribeVersionCommand();
            commandExecuter.Execute(describeVersionCommand);
            return describeVersionCommand.Version;
        }

        public void AddKeyspace(Keyspace keyspace)
        {
            var addKeyspaceCommand = new AddKeyspaceCommand(keyspace);
            commandExecuter.ExecuteSchemeUpdateCommandOnce(addKeyspaceCommand);
            logger.InfoFormat("Keyspace adding result: {0}", addKeyspaceCommand.Output);
        }

        public void WaitUntilSchemeAgreementIsReached(TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            do
            {
                var schemaAgreementCommand = new SchemaAgreementCommand();
                commandExecuter.Execute(schemaAgreementCommand);
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

        private bool IsSystemKeyspace(string keyspaceName)
        {
            return systemKeyspaceNames.Any(s => s.Equals(keyspaceName, StringComparison.OrdinalIgnoreCase));
        }

        private readonly string[] systemKeyspaceNames = new[] {"system", "system_auth", "system_traces"};

        private readonly ICommandExecuter commandExecuter;

        private readonly ILog logger = LogManager.GetLogger(typeof(ClusterConnection));
    }
}