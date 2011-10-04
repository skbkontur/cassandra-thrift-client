using System;
using System.Collections.Generic;

using System.Text;
using Apache.Cassandra;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Ported Cassandra Consistency level for commands over the conection.
    /// <remarks>By convention this values must match Cassandra Consistency Level</remarks>
    /// </summary>
    public enum AquilesConsistencyLevel : int
    {
        /// <summary>
        /// Write: Ensure that the write has been written to at least 1 node, including hinted recipients
        /// Read: Will return the record returned by the first node to respond. A consistency check is always done in a background thread to fix any consistency issues when ConsistencyLevel.ONE is used. This means subsequent calls will have correct data even if the initial read gets an older value. (This is called read repair.)
        /// </summary>
        ONE = 1,
        /// <summary>
        /// Write: Ensure that the write has been written to at least 1 node's commit log and memory table before responding to the client. 
        /// Read: Will query all nodes and return the record with the most recent timestamp once it has at least a majority of replicas reported. Again, the remaining replicas will be checked in the background.
        /// </summary>
        QUORUM = 2,
        /// <summary>
        /// undocumented
        /// </summary>
        LOCAL_QUORUM = 3,
        /// <summary>
        /// undocumented
        /// </summary>
        EACH_QUORUM = 4,
        /// <summary>
        /// Write: Ensure that the write is written to all ReplicationFactor nodes before responding to the client. Any unresponsive nodes will fail the operation
        /// Read: Will query all nodes and return the record with the most recent timestamp once all nodes have replied. Any unresponsive nodes will fail the operation
        /// </summary>
        ALL = 5,
        /// <summary>
        /// Write: Ensure that the write has been written to at least 1 node, including hinted recipients. 
        /// Read: Not supported. You probably want ONE instead. 
        /// </summary>
        ANY = 6,
    }
}
