using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Commands.System.Write
{
    internal class DropColumnFamilyCommand : KeyspaceColumnFamilyDependantCommandBase, IFierceCommand
    {
        public DropColumnFamilyCommand(string keyspace, string columnFamily)
            : base(keyspace, columnFamily)
        {
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Output = cassandraClient.system_drop_column_family(columnFamily);
        }

        public string Output { get; private set; }
    }
}