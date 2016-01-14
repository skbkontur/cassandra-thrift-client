using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class Keyspace
    {
        public Dictionary<string, ColumnFamily> ColumnFamilies { get; set; }
        public string Name { get; set; }
        public IReplicationStrategy ReplicationStrategy { get; set; }
    }

    internal static class KeyspaceExtensions
    {
        private static readonly IReplicationStrategyFactory replicationStrategyFactory = new ReplicationStrategyFactory();

        public static KsDef ToCassandraKsDef(this Keyspace keyspace)
        {
            if(keyspace == null)
                return null;
            var columnFamilies = (keyspace.ColumnFamilies ?? new Dictionary<string, ColumnFamily>())
                .Values.Select(family => family.ToCassandraCfDef(keyspace.Name)).ToList();

            return new KsDef
                {
                    Name = keyspace.Name,
                    Strategy_class = keyspace.ReplicationStrategy.Name,
                    Strategy_options = keyspace.ReplicationStrategy.StrategyOptions,
                    Cf_defs = columnFamilies
                };
        }

        public static Keyspace FromCassandraKsDef(this KsDef ksDef)
        {
            if(ksDef == null)
                return null;
            var columnFamilies = (ksDef.Cf_defs ?? new List<CfDef>()).ToDictionary(def => def.Name, def => def.FromCassandraCfDef());
            var keyspace = new Keyspace
                {
                    Name = ksDef.Name,
                    ReplicationStrategy = replicationStrategyFactory.Create(ksDef.Strategy_class, ksDef.Strategy_options),
                    ColumnFamilies = columnFamilies,
                };
            return keyspace;
        }
    }
}