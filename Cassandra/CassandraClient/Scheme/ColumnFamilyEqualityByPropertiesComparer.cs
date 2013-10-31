using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Scheme
{
    internal class ColumnFamilyEqualityByPropertiesComparer
    {
        public bool Equals(ColumnFamily x, ColumnFamily y)
        {
            return
                x.ReadRepairChance.Equals(y.ReadRepairChance);
        }
    }
}