using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Read;
using SKBKontur.Cassandra.CassandraClient.Commands.System.Write;
using SKBKontur.Cassandra.CassandraClient.Core;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    internal class KeyspaceConnection : IKeyspaceConnection
    {
        public KeyspaceConnection(ICommandExecutor<IFierceCommand> commandExecutor,
                                  string keyspaceName)
        {
            this.commandExecutor = commandExecutor;
            this.keyspaceName = keyspaceName;
        }

        public void AddColumnFamily(ColumnFamily columnFamily)
        {
            commandExecutor.Execute(new AddColumnFamilyCommand(keyspaceName, columnFamily));
        }

        public void AddColumnFamily(string columnFamilyName)
        {
            commandExecutor.Execute(new AddColumnFamilyCommand(keyspaceName, new ColumnFamily {Name = columnFamilyName}));
        }

        public Keyspace DescribeKeyspace()
        {
            var describeKeyspaceCommand = new DescribeKeyspaceCommand(keyspaceName);
            commandExecutor.Execute(describeKeyspaceCommand);
            return describeKeyspaceCommand.KeyspaceInformation;
        }

        public void UpdateColumnFamily(ColumnFamily columnFamily)
        {
            commandExecutor.Execute(new UpdateColumnFamilyCommand(keyspaceName, columnFamily));
        }

        public void RemoveColumnFamily(string columnFamily)
        {
            commandExecutor.Execute(new DropColumnFamilyCommand(keyspaceName, columnFamily));
        }

        private readonly ICommandExecutor<IFierceCommand> commandExecutor;
        private readonly string keyspaceName;
    }
}