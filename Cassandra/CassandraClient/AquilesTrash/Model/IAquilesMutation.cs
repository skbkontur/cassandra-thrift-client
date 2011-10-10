using System;
using System.Collections.Generic;

using System.Text;
using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Interface of Mutations
    /// </summary>
    public interface IAquilesMutation : IAquilesObject
    {
        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter.
        /// Note: Mutations are exclusive for 1 operation, so there is no need to validate for a type of operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        void Validate();
    }
}
