using System.Collections.Generic;

using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System
{
    public class SchemaAgreementCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.describe_schema_versions();
        }

        public Dictionary<string, List<string>> Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}