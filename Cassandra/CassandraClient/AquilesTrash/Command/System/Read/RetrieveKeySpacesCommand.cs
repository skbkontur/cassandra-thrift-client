using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Read
{
    public class RetrieveKeyspacesCommand : CommandBase
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            List<KsDef> keySpaces = cassandraClient.describe_keyspaces();
            Keyspaces = BuildKeyspaces(keySpaces);
        }

        public override bool IsFierce { get { return true; } }
        public List<AquilesKeyspace> Keyspaces { get; private set; }

        private static List<AquilesKeyspace> BuildKeyspaces(IEnumerable<KsDef> keySpaces)
        {
            if(keySpaces == null) return null;
            var convertedKeyspaces = keySpaces.Select(ModelConverterHelper.Convert<AquilesKeyspace, KsDef>).ToList();
            return convertedKeyspaces;
        }
    }
}