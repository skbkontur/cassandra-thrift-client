using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Write
{
    internal class AddColumnFamilyCommand : KeyspaceDependantCommandBase, ISchemeUpdateCommand
    {
        public AddColumnFamilyCommand(string keyspace, ColumnFamily columnFamilyDefinition)
            : base(keyspace)
        {
            this.columnFamilyDefinition = columnFamilyDefinition;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.system_add_column_family(columnFamilyDefinition.ToCassandraCfDef(keyspace));
        }

        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
        private readonly ColumnFamily columnFamilyDefinition;
    }
}