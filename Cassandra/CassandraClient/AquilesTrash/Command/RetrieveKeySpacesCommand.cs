using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command
{
    public class RetrieveKeyspacesCommand : AbstractCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
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