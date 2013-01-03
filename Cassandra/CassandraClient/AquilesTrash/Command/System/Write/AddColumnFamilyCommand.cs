using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Write
{
    public class AddColumnFamilyCommand : KeyspaceDependantCommandBase
    {
        public AddColumnFamilyCommand(string keyspace, AquilesColumnFamily columnFamilyDefinition)
            : base(keyspace)
        {
            this.columnFamilyDefinition = columnFamilyDefinition;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var columnFamily = ModelConverterHelper.Convert<AquilesColumnFamily, CfDef>(columnFamilyDefinition);
            Output = cassandraClient.system_add_column_family(columnFamily);
        }

        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
        private readonly AquilesColumnFamily columnFamilyDefinition;
    }
}