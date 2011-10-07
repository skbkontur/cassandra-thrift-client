using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command.System
{
    public class AddColumnFamilyCommand : AbstractKeyspaceDependantCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            var columnFamily = ModelConverterHelper.Convert<AquilesColumnFamily, CfDef>(ColumnFamilyDefinition);
            Output = cassandraClient.system_add_column_family(columnFamily);
        }

        public override void ValidateInput()
        {
            ColumnFamilyDefinition.ValidateForInsertOperation();
        }

        public AquilesColumnFamily ColumnFamilyDefinition { get; set; }

        public string Output { get; private set; }
    }
}