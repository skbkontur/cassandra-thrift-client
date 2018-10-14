using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Write
{
    internal class AddColumnFamilyCommand : KeyspaceDependantCommandBase, IFierceCommand
    {
        public AddColumnFamilyCommand(string keyspace, ColumnFamily columnFamilyDefinition)
            : base(keyspace)
        {
            this.columnFamilyDefinition = columnFamilyDefinition;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Output = cassandraClient.system_add_column_family(columnFamilyDefinition.ToCassandraCfDef(keyspace));
        }

        public string Output { get; private set; }
        private readonly ColumnFamily columnFamilyDefinition;
    }
}