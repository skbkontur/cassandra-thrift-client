using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Write
{
    public class SchemaAgreementCommand : CommandBase
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.describe_schema_versions();
        }

        public Dictionary<string, List<string>> Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}