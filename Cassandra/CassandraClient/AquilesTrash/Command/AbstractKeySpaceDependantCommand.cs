using System;
using System.Collections.Generic;

using System.Text;
using Apache.Cassandra;

using CassandraClient.AquilesTrash.Model;
using System.Globalization;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Abstract class for an AquilesCommand that needs to have a Keyspace
    /// </summary>
    public abstract class AbstractKeyspaceDependantCommand : CassandraClient.AquilesTrash.Command.AbstractCommand
    {
        /// <summary>
        /// Indicates if this command applies only to a keyspace
        /// </summary>
        public sealed override bool isKeyspaceDependant
        {
            get { return true; }
        }
    }
}
