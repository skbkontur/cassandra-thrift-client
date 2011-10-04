using System;
using System.Collections.Generic;

using System.Text;

namespace CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Information of a Cluster
    /// </summary>
    public class CassandraClusterInformation
    {
        /// <summary>
        /// get or set the Partioner used on this cluster
        /// </summary>
        public string Partioner
        {
            get;
            set;
        }
    }
}
