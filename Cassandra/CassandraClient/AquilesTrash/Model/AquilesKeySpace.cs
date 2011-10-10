using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles representation of a Keyspace
    /// </summary>
    public class AquilesKeyspace : IAquilesObject
    {

        /// <summary>
        /// org.apache.cassandra.locator.SimpleStrategy
        /// </summary>
        public const string SIMPLESTRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
        /// <summary>
        /// org.apache.cassandra.locator.NetworkTopologyStrategy
        /// </summary>
        public const string NETWORKTOPOLOGYSTRATEGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";
        /// <summary>
        /// org.apache.cassandra.locator.OldNetworkTopologyStrategy
        /// </summary>
        public const string OLDNETWORKTOPOLOGYSTRATEGY = "org.apache.cassandra.locator.OldNetworkTopologyStrategy";

        /// <summary>
        /// Creates an AquilesKeyspace with predefined default values
        /// </summary>
        public AquilesKeyspace()
        {
            this.ReplicationPlacementStrategy = SIMPLESTRATEGY;
        }

        /// <summary>
        /// get or set Name.
        /// <remarks>"system" and "definitions" are reserved for Cassandra Internals.</remarks>
        /// </summary>
        public string Name
        {
            get;
            set;
        }

        /// <summary>
        /// get or set Dictionary of ColumnFamilies where key is the name of the ColumnFamily
        /// </summary>
        public Dictionary<string, AquilesColumnFamily> ColumnFamilies
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the Number of replicas of each row
        /// </summary>
        public int ReplicationFactor
        {
            get;
            set;
        }
        /// <summary>
        /// get or set the class that determines how replicas are distributed among nodes
        /// </summary>
        public string ReplicationPlacementStrategy
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the configuration information for the strategy selected
        /// </summary>
        public Dictionary<string, string> ReplicationPlacementStrategyOptions
        {
            get;
            set;
        }
      
        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an insert Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForInsertOperation()
        {
            this.ValidateNotNullOrEmptyName();
            this.ValidateNotNullOrEmptyReplicationPlacementStrategy();
            this.ValidateReplicationFactor();
            if (this.ColumnFamilies != null)
            {
                foreach (AquilesColumnFamily columnFamily in this.ColumnFamilies.Values)
                {
                    columnFamily.ValidateForInsertOperation();
                }
            }

            //TODO validate that the keyspace at least has a child columnfamily??
        }
        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an deletation Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForDeletationOperation()
        {
            //throw new NotImplementedException();
        }
        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForSetOperation()
        {
            //throw new NotImplementedException();
        }
        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in a Query Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForQueryOperation()
        {
            //throw new NotImplementedException();
        }

        

        private void ValidateReplicationFactor()
        {
            if (this.ReplicationFactor < 0)
            {
                throw new AquilesCommandParameterException("Replication Factor must be equal or higher than 0.");
            }
        }

        private void ValidateNotNullOrEmptyReplicationPlacementStrategy()
        {
            if (String.IsNullOrEmpty(this.ReplicationPlacementStrategy))
            {
                throw new AquilesCommandParameterException("Replication Placement Strategy cannot be null or empty");
            }
        }

        private void ValidateNotNullOrEmptyName()
        {
            if (String.IsNullOrEmpty(this.Name))
            {
                throw new AquilesCommandParameterException("Name cannot be null or empty");
            }
        }
    }
}
