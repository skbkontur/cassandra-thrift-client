using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class RetrieveKeyspaceDistributionComand : AbstractKeyspaceDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            var results = cassandraClient.describe_ring(Keyspace);
            BuildOut(results);
        }

        public override void ValidateInput(ICassandraLogger logger)
        {
            if(String.IsNullOrEmpty(Keyspace))
                throw new AquilesCommandParameterException("Keyspace must be not null or empty.");
        }

        public string Keyspace { set; protected get; }
        public List<AquilesTokenRange> Output { get; private set; }

        private void BuildOut(IEnumerable<TokenRange> results)
        {
            if(results == null) return;
            Output = results.Select(ModelConverterHelper.Convert<AquilesTokenRange, TokenRange>).ToList();
        }
    }
}