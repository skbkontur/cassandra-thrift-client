using System.Collections.Generic;

using Apache.Cassandra;

namespace CassandraClient.AquilesTrash.Command.System
{
    public class SchemaAgreementCommand : AbstractCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.describe_schema_versions();
        }

        public Dictionary<string, List<string>> Output { get; private set; }
    }
}