using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Read
{
    internal class DescribeKeyspaceCommand : KeyspaceDependantCommandBase, IFierceCommand
    {
        public DescribeKeyspaceCommand(string keyspace)
            : base(keyspace)
        {
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            var keyspaceDescription = cassandraClient.describe_keyspace(keyspace);
            KeyspaceInformation = keyspaceDescription.FromCassandraKsDef();
        }

        public Keyspace KeyspaceInformation { get; private set; }
    }
}