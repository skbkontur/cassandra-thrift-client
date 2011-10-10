using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter.Model;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesKeyspace
    /// </summary>
    public class AquilesKeyspaceConverter : IThriftConverter<AquilesKeyspace, KsDef>
    {
        /// <summary>
        /// Transform AquilesKeyspace structure into KsDef
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public KsDef Transform(AquilesKeyspace objectA)
        {
            KsDef keyspace = new KsDef();
            keyspace.Name = objectA.Name;
            keyspace.Replication_factor = objectA.ReplicationFactor;
            keyspace.Strategy_class = objectA.ReplicationPlacementStrategy;
            keyspace.Strategy_options = objectA.ReplicationPlacementStrategyOptions;
            if (objectA.ColumnFamilies == null)
            {
                keyspace.Cf_defs = new List<CfDef>();
            }
            else
            {
                keyspace.Cf_defs = new List<CfDef>(objectA.ColumnFamilies.Count);
                Dictionary<string, AquilesColumnFamily>.ValueCollection.Enumerator columnFamilyIterator = objectA.ColumnFamilies.Values.GetEnumerator();
                while (columnFamilyIterator.MoveNext())
                {
                    keyspace.Cf_defs.Add(ModelConverterHelper.Convert<AquilesColumnFamily,CfDef>(columnFamilyIterator.Current));
                }
            }
            return keyspace;
        }

        /// <summary>
        /// Transform KsDef structure into AquilesKeyspace
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public AquilesKeyspace Transform(KsDef objectB)
        {
            AquilesKeyspace keyspace = new AquilesKeyspace();
            keyspace.Name = objectB.Name;
            keyspace.ReplicationFactor = objectB.Replication_factor;
            keyspace.ReplicationPlacementStrategy = objectB.Strategy_class;
            keyspace.ReplicationPlacementStrategyOptions = objectB.Strategy_options;
            if (objectB.Cf_defs == null)
            {
                keyspace.ColumnFamilies = new Dictionary<string, AquilesColumnFamily>();
            }
            else
            {
                AquilesColumnFamily columnFamilyDefinition = null;
                keyspace.ColumnFamilies = new Dictionary<string, AquilesColumnFamily>(objectB.Cf_defs.Count);
                foreach (CfDef cfDef in objectB.Cf_defs)
                {
                    columnFamilyDefinition = ModelConverterHelper.Convert<AquilesColumnFamily,CfDef>(cfDef);
                    keyspace.ColumnFamilies.Add(columnFamilyDefinition.Name, columnFamilyDefinition);
                }
            }
            return keyspace;
        }

        
    }
}
