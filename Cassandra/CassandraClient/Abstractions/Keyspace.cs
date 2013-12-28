using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class Keyspace
    {
        public Dictionary<string, ColumnFamily> ColumnFamilies { get; set; }
        public string Name { get; set; }
        public int ReplicationFactor { get; set; }
        public string ReplicaPlacementStrategy { get; set; }
    }

    internal static class KeyspaceExtensions
    {
        public static KsDef ToCassandraKsDef(this Keyspace keyspace)
        {
            if(keyspace == null)
                return null;
            var columnFamilies = (keyspace.ColumnFamilies ?? new Dictionary<string, ColumnFamily>())
                .Values.Select(family => family.ToCassandraCfDef(keyspace.Name)).ToList();
            var ksDef = new KsDef
                {
                    Name = keyspace.Name,
                    Replication_factor = keyspace.ReplicationFactor,
                    Strategy_class = keyspace.ReplicaPlacementStrategy,
                    Cf_defs = columnFamilies,
                };
            if(keyspace.ReplicaPlacementStrategy == ReplicaPlacementStrategy.Simple.ToStringValue())
                ksDef.Strategy_options = new Dictionary<string, string> {{"replication_factor", keyspace.ReplicationFactor.ToString(CultureInfo.InvariantCulture)}};
            return ksDef;
        }

        public static Keyspace FromCassandraKsDef(this KsDef ksDef)
        {
            if(ksDef == null)
                return null;
            var columnFamilies = (ksDef.Cf_defs ?? new List<CfDef>()).ToDictionary(def => def.Name, def => def.FromCassandraCfDef());
            var keyspace = new Keyspace
                {
                    Name = ksDef.Name,
                    ReplicaPlacementStrategy = ksDef.Strategy_class,
                    ColumnFamilies = columnFamilies,
                };
            if(ksDef.Strategy_options != null && ksDef.Strategy_options.ContainsKey("replication_factor"))
                keyspace.ReplicationFactor = int.Parse(ksDef.Strategy_options["replication_factor"]);
            return keyspace;
        }
    }
}