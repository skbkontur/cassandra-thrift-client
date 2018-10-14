using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Write
{
    internal class SchemaAgreementCommand : CommandBase, IFierceCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Output = cassandraClient.describe_schema_versions();
        }

        public Dictionary<string, List<string>> Output { get; private set; }
    }
}