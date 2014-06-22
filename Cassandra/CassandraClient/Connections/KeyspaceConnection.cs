using System.Collections.Generic;
using System.Linq;
using System.Text;

using log4net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Read;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Write;
using SKBKontur.Cassandra.CassandraClient.Core;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    internal class KeyspaceConnection : IKeyspaceConnection
    {
        public KeyspaceConnection(ICommandExecuter commandExecuter,
                                  string keyspaceName)
        {
            this.commandExecuter = commandExecuter;
            this.keyspaceName = keyspaceName;
        }

        public void AddColumnFamily(ColumnFamily columnFamily)
        {
            commandExecuter.Execute(new AddColumnFamilyCommand(keyspaceName, columnFamily));
            WaitUntilAgreementIsReached();
        }

        public void AddColumnFamily(string columnFamilyName)
        {
            commandExecuter.Execute(new AddColumnFamilyCommand(keyspaceName,
                                                               new ColumnFamily {Name = columnFamilyName}));
            WaitUntilAgreementIsReached();
        }

        public Keyspace DescribeKeyspace()
        {
            var describeKeyspaceCommand = new DescribeKeyspaceCommand(keyspaceName);
            commandExecuter.Execute(describeKeyspaceCommand);
            return describeKeyspaceCommand.KeyspaceInformation;
        }

        public void UpdateColumnFamily(ColumnFamily columnFamily)
        {
            commandExecuter.Execute(new UpdateColumnFamilyCommand(keyspaceName, columnFamily));
            WaitUntilAgreementIsReached();
        }

        public void RemoveColumnFamily(string columnFamily)
        {
            commandExecuter.Execute(new DropColumnFamilyCommand(keyspaceName, columnFamily));
            WaitUntilAgreementIsReached();
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
            {
                stringBuilder.AppendLine(string.Format("\tVerson: {0}, Nodes: {1}", agreeds.Key,
                                                       agreeds.Value.Aggregate("",
                                                                               (s1, s2) =>
                                                                               string.Format("{0}, {1}", s1, s2))));
            }
            logger.Info(stringBuilder.ToString());
        }

        private readonly ICommandExecuter commandExecuter;
        private readonly string keyspaceName;

        private readonly ILog logger = LogManager.GetLogger(typeof(KeyspaceConnection));
    }
}