using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command
{
    public class RetrieveKeyspaceDistributionComand : AbstractKeyspaceDependantCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            var results = cassandraClient.describe_ring(Keyspace);
            BuildOut(results);
        }

        public override void ValidateInput()
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