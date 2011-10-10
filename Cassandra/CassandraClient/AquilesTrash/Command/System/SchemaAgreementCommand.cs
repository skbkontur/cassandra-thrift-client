using System.Collections.Generic;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System
{
    public class SchemaAgreementCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            Output = cassandraClient.describe_schema_versions();
        }

        public Dictionary<string, List<string>> Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}