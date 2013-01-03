using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Read
{
    public class RetrieveKeyspaceDistributionComand : KeyspaceDependantCommandBase
    {
        public RetrieveKeyspaceDistributionComand(string keyspace)
            : base(keyspace)
        {
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var results = cassandraClient.describe_ring(keyspace);
            BuildOut(results);
        }

        public override bool IsFierce { get { return true; } }
        public List<AquilesTokenRange> Output { get; private set; }

        private void BuildOut(IEnumerable<TokenRange> results)
        {
            if(results == null) return;
            Output = results.Select(ModelConverterHelper.Convert<AquilesTokenRange, TokenRange>).ToList();
        }
    }
}