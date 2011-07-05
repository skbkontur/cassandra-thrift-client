using System.Linq;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;

namespace CassandraClient.StorageCore
{
    public abstract class IdSetRepository : IIdSetRepository
    {
        public IdSetRepository(ICassandraCluster cassandraCluster, ICassandraCoreSettings settings, string columnFamilyName)
        {
            this.cassandraCluster = cassandraCluster;
            this.settings = settings;
            this.columnFamilyName = columnFamilyName;
        }

        public void Write(params string[] ids)
        {
            if(ids == null || ids.Length == 0)
                return;
            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(settings.KeyspaceName, columnFamilyName))
                conn.AddBatch("Ids", ids.Select(id => new Column {Name = id, Value = new byte[] {0}}));
        }

        public string[] Read(int maxCount, string startId)
        {
            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(settings.KeyspaceName, columnFamilyName))
            {
                var columns = conn.GetRow("Ids", startId, maxCount);
                return columns.Select(col => col.Name).ToArray();
            }
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraCoreSettings settings;
        private readonly string columnFamilyName;
    }
}