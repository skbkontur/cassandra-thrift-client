using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public class Keyspace
    {
        public Keyspace()
        {
            DurableWrites = true;
        }

        public string Name { get; set; }
        public bool DurableWrites { get; set; }
        public IReplicationStrategy ReplicationStrategy { get; set; }
        public Dictionary<string, ColumnFamily> ColumnFamilies { get; set; }
    }

    internal static class KeyspaceExtensions
    {
        public static KsDef ToCassandraKsDef(this Keyspace keyspace)
        {
            if (keyspace == null)
                return null;
            return new KsDef
                {
                    Name = keyspace.Name,
                    Durable_writes = keyspace.DurableWrites,
                    Strategy_class = keyspace.ReplicationStrategy.Name,
                    Strategy_options = keyspace.ReplicationStrategy.StrategyOptions,
                    Cf_defs = (keyspace.ColumnFamilies ?? new Dictionary<string, ColumnFamily>()).Values.Select(family => family.ToCassandraCfDef(keyspace.Name)).ToList(),
                };
        }

        public static Keyspace FromCassandraKsDef(this KsDef ksDef)
        {
            if (ksDef == null)
                return null;
            var keyspace = new Keyspace
                {
                    Name = ksDef.Name,
                    DurableWrites = ksDef.Durable_writes,
                    ReplicationStrategy = replicationStrategyFactory.Create(ksDef),
                    ColumnFamilies = (ksDef.Cf_defs ?? new List<CfDef>()).ToDictionary(def => def.Name, def => def.FromCassandraCfDef()),
                };
            return keyspace;
        }

        private static readonly IReplicationStrategyFactory replicationStrategyFactory = ReplicationStrategyFactory.FactoryInstance;
    }
}