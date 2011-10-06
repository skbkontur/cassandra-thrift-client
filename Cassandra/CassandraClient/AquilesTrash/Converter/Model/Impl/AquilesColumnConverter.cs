using Apache.Cassandra;

using CassandraClient.AquilesTrash.Model;
using CassandraClient.Core;

namespace CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesColumn
    /// </summary>
    public class AquilesColumnConverter : IThriftConverter<AquilesColumn, Column>
    {
        /// <summary>
        /// Transform AquilesColumn structure into Column
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public Column Transform(AquilesColumn objectA)
        {
            Column cassandraColumn = new Column();
            cassandraColumn.Name = objectA.ColumnName;
            cassandraColumn.Value = objectA.Value;
            if (objectA.TTL.HasValue)
            {
                cassandraColumn.Ttl = objectA.TTL.Value;
            }

            if (objectA.Timestamp == null)
            {
                cassandraColumn.Timestamp = DateTimeService.UtcNow.Ticks;
            }
            else
            {
                cassandraColumn.Timestamp = objectA.Timestamp.Value;
            }
            return cassandraColumn;
        }

        /// <summary>
        /// Transform Column structure into AquilesColumn
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public AquilesColumn Transform(Column objectB)
        {
            AquilesColumn aquilesColumn = new AquilesColumn();
            aquilesColumn.ColumnName = objectB.Name;
            aquilesColumn.Value = objectB.Value;
            if (objectB.__isset.ttl)
            {
                aquilesColumn.TTL = objectB.Ttl;
            }
            aquilesColumn.Timestamp = objectB.Timestamp;
            return aquilesColumn;
        }

        
    }
}
