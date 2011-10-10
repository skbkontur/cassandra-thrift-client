using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System
{
    public class AddColumnFamilyCommand : AbstractKeyspaceDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            var columnFamily = ModelConverterHelper.Convert<AquilesColumnFamily, CfDef>(ColumnFamilyDefinition);
            Output = cassandraClient.system_add_column_family(columnFamily);
        }

        public override void ValidateInput(ICassandraLogger logger)
        {
            ColumnFamilyDefinition.ValidateForInsertOperation();
        }

        public AquilesColumnFamily ColumnFamilyDefinition { get; set; }
        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}