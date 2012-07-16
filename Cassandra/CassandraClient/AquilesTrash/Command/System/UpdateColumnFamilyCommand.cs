using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System
{
    public class UpdateColumnFamilyCommand : AbstractKeyspaceDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var columnFamily = ModelConverterHelper.Convert<AquilesColumnFamily, CfDef>(ColumnFamilyDefinition);
            Output = cassandraClient.system_update_column_family(columnFamily);
        }

        public override void ValidateInput()
        {
            ColumnFamilyDefinition.ValidateForInsertOperation();
        }

        public AquilesColumnFamily ColumnFamilyDefinition { private get; set; }
        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}