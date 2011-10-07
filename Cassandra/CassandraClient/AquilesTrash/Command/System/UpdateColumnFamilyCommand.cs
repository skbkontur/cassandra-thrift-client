using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command.System
{
    public class UpdateColumnFamilyCommand : AbstractKeyspaceDependantCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
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