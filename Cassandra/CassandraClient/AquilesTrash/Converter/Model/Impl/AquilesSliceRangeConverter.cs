using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter.Model;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesSliceRange
    /// </summary>
    public class AquilesSliceRangeConverter : IThriftConverter<AquilesSliceRange, SliceRange>
    {
        /// <summary>
        /// Transform AquilesSliceRange structure into SliceRange
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public SliceRange Transform(AquilesSliceRange objectA)
        {
            SliceRange sliceRange = new SliceRange();
            sliceRange.Count = objectA.Count;
            if (objectA.FinishColumn == null)
            {
                sliceRange.Finish = new byte[0];
            }
            else
            {
                sliceRange.Finish = objectA.FinishColumn;
            }
            if (objectA.StartColumn == null)
            {
                sliceRange.Start = new byte[0];
            }
            else
            {
                sliceRange.Start = objectA.StartColumn;
            }
            sliceRange.Reversed = objectA.Reversed;

            return sliceRange;
        }

        /// <summary>
        /// Transform SliceRange structure into AquilesSliceRange
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public AquilesSliceRange Transform(SliceRange objectB)
        {
            AquilesSliceRange sliceRange = new AquilesSliceRange();
            sliceRange.Count = objectB.Count;
            if (objectB.Finish != null && objectB.Finish.Length > 0)
            {
                sliceRange.FinishColumn = objectB.Finish;
            }
            if (objectB.Start != null && objectB.Start.Length > 0)
            {
                sliceRange.StartColumn = objectB.Start;
            }
            sliceRange.Reversed = objectB.Reversed;

            return sliceRange;
        }

        

    }
}
