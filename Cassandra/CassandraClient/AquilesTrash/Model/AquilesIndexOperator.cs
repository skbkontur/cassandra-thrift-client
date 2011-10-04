using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to support for Cassandra IndexOperator structure
    /// </summary>
    public enum AquilesIndexOperator : int
    {
        /// <summary>
        /// Equals
        /// </summary>
        EQ = 0,
        /// <summary>
        /// Greater than or Equals
        /// </summary>
        GTE = 1,
        /// <summary>
        /// Greater than
        /// </summary>
        GT = 2,
        /// <summary>
        /// Lower than or Equals
        /// </summary>
        LTE = 3,
        /// <summary>
        /// Lower than
        /// </summary>
        LT = 4
    }
}
