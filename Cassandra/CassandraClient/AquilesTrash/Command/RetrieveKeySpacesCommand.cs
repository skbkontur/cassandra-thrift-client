using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class RetrieveKeyspacesCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            List<KsDef> keySpaces = cassandraClient.describe_keyspaces();
            Keyspaces = BuildKeyspaces(keySpaces);
        }

        public List<AquilesKeyspace> Keyspaces { get; private set; }

        private static List<AquilesKeyspace> BuildKeyspaces(IEnumerable<KsDef> keySpaces)
        {
            if (keySpaces == null) return null;
            var convertedKeyspaces = keySpaces.Select(ModelConverterHelper.Convert<AquilesKeyspace, KsDef>).ToList();
            return convertedKeyspaces;
        }
    }
}