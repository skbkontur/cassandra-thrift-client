using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Write
{
    public class UpdateColumnFamilyCommand : KeyspaceDependantCommandBase
    {
        public UpdateColumnFamilyCommand(string keyspace, AquilesColumnFamily columnFamilyDefinition)
            : base(keyspace)
        {
            this.columnFamilyDefinition = columnFamilyDefinition;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var columnFamily = ModelConverterHelper.Convert<AquilesColumnFamily, CfDef>(columnFamilyDefinition);
            Output = cassandraClient.system_update_column_family(columnFamily);
        }

        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
        private readonly AquilesColumnFamily columnFamilyDefinition;
    }
}