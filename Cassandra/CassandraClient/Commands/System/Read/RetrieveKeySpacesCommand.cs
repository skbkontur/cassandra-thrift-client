using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Read
{
    internal class RetrieveKeyspacesCommand : CommandBase
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var keySpaces = cassandraClient.describe_keyspaces();
            Keyspaces = BuildKeyspaces(keySpaces);
        }

        public override bool IsFierce { get { return true; } }
        public List<Keyspace> Keyspaces { get; private set; }

        private static List<Keyspace> BuildKeyspaces(IEnumerable<KsDef> keySpaces)
        {
            if(keySpaces == null) return null;
            var convertedKeyspaces = keySpaces.Select(def => def.FromCassandraKsDef()).ToList();
            return convertedKeyspaces;
        }
    }
}