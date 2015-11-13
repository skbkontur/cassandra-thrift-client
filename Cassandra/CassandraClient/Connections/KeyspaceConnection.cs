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
            commandExecuter.ExecuteSchemeUpdateCommandOnce(new AddColumnFamilyCommand(keyspaceName, columnFamily));
        }

        public void AddColumnFamily(string columnFamilyName)
        {
            commandExecuter.ExecuteSchemeUpdateCommandOnce(new AddColumnFamilyCommand(keyspaceName, new ColumnFamily {Name = columnFamilyName}));
        }

        public Keyspace DescribeKeyspace()
        {
            var describeKeyspaceCommand = new DescribeKeyspaceCommand(keyspaceName);
            commandExecuter.Execute(describeKeyspaceCommand);
            return describeKeyspaceCommand.KeyspaceInformation;
        }

        public void UpdateColumnFamily(ColumnFamily columnFamily)
        {
            commandExecuter.ExecuteSchemeUpdateCommandOnce(new UpdateColumnFamilyCommand(keyspaceName, columnFamily));
        }

        public void RemoveColumnFamily(string columnFamily)
        {
            commandExecuter.ExecuteSchemeUpdateCommandOnce(new DropColumnFamilyCommand(keyspaceName, columnFamily));
        }

        private readonly ICommandExecuter commandExecuter;
        private readonly string keyspaceName;
    }
}