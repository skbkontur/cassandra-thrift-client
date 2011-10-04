using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using CassandraClient.AquilesTrash.Model;

using CassandraClient.AquilesTrash.Converter.Model;

namespace CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesKeyRange
    /// </summary>
    public class AquilesKeyRangeConverter : IThriftConverter<AquilesKeyRange, KeyRange>
    {
        #region IThriftConverter<AquilesKeyRange,KeyRange> Members

        /// <summary>
        /// Transform AquilesKeyRange structure into KeyRange
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public KeyRange Transform(AquilesKeyRange objectA)
        {
            KeyRange keyRange = new KeyRange();
            keyRange.Count = objectA.Count;
            keyRange.Start_key = objectA.StartKey;
            keyRange.End_key = objectA.EndKey;

            return keyRange;
        }

        /// <summary>
        /// Transform KeyRange structure into AquilesKeyRange
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public AquilesKeyRange Transform(KeyRange objectB)
        {
            AquilesKeyRange keyRange = new AquilesKeyRange();
            keyRange.Count = objectB.Count;
            keyRange.StartKey = objectB.Start_key;
            keyRange.EndKey = objectB.End_key;

            return keyRange;
        }

        #endregion
    }
}
