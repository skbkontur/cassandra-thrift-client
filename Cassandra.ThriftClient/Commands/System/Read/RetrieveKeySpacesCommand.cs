using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Read
{
    internal class RetrieveKeyspacesCommand : CommandBase, IFierceCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var keySpaces = cassandraClient.describe_keyspaces();
            Keyspaces = BuildKeyspaces(keySpaces);
        }

        public List<Keyspace> Keyspaces { get; private set; }

        private static List<Keyspace> BuildKeyspaces(IEnumerable<KsDef> keySpaces)
        {
            if (keySpaces == null) return null;
            var convertedKeyspaces = keySpaces.Where(x => !IsSystemKeyspace(x.Name)).Select(def => def.FromCassandraKsDef()).ToList();
            return convertedKeyspaces;
        }

        private static bool IsSystemKeyspace(string keyspaceName)
        {
            return systemKeyspaceNames.Any(s => s.Equals(keyspaceName, StringComparison.OrdinalIgnoreCase));
        }

        private static readonly string[] systemKeyspaceNames = {"system", "system_auth", "system_traces", "system_schema", "system_distributed"};
    }
}