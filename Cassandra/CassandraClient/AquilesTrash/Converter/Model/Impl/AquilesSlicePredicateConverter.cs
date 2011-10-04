using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using CassandraClient.AquilesTrash.Model;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Converter.Model;

namespace CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesSlicePredicate
    /// </summary>
    public class AquilesSlicePredicateConverter : IThriftConverter<AquilesSlicePredicate, SlicePredicate>
    {
        #region IThriftConverter<AquilesSlicePredicate,SlicePredicate> Members

        /// <summary>
        /// Transform AquilesSlicePredicate structure into SlicePredicate
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public SlicePredicate Transform(AquilesSlicePredicate objectA)
        {
            SlicePredicate predicate = new SlicePredicate();
            if (objectA.SliceRange != null)
            {
                predicate.Slice_range = ModelConverterHelper.Convert<AquilesSliceRange,SliceRange>(objectA.SliceRange);
            }
            if (objectA.Columns != null)
            {
                predicate.Column_names = new List<byte[]>(objectA.Columns.Count);
                foreach (byte[] column in objectA.Columns)
                {
                    predicate.Column_names.Add(column);
                }
            }
            return predicate;
        }

        /// <summary>
        /// Transform SlicePredicate structure into AquilesSlicePredicate
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public AquilesSlicePredicate Transform(SlicePredicate objectB)
        {
            AquilesSlicePredicate predicate = new AquilesSlicePredicate();
            if (objectB.Slice_range != null)
            {
                predicate.SliceRange = ModelConverterHelper.Convert<AquilesSliceRange,SliceRange>(objectB.Slice_range);
            }
            if (objectB.Column_names != null)
            {
                predicate.Columns = new List<byte[]>(objectB.Column_names.Count);
                foreach (byte[] column in objectB.Column_names)
                {
                    predicate.Columns.Add(column);
                }
            }
            return predicate;
        }

        #endregion
    }
}
