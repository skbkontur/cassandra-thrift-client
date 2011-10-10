using System;
using System.Collections.Generic;

using System.Text;
using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to contains Cassandra Key Range structure (only support for a Token range get)
    /// <remarks>EndToken might be equal of smaller than the StartToken. If EndToken is equal to StartToken then you will get the full ring</remarks>
    /// </summary>
    public class AquilesTokenRange : IAquilesObject
    {
        /// <summary>
        /// get or set the Start Token (excluded in the command response)
        /// </summary>
        public string StartToken
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the End Token
        /// </summary>
        public string EndToken
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the list of endpoints (nodes) that replace data in the TokenRange
        /// </summary>
        public List<string> Endpoints
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
            this.Validate();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an deletation Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForDeletationOperation()
        {
            this.Validate();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForSetOperation()
        {
            this.Validate();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForQueryOperation()
        {
            this.Validate();
        }

        private void Validate()
        {
            if (this.Endpoints == null || this.Endpoints.Count <= 0)
            {
                throw new AquilesCommandParameterException("List of valid endpoints (nodes) is required.");
            }

            if (String.IsNullOrEmpty(this.StartToken))
            {
                throw new AquilesCommandParameterException("StartToken must not be null or empty.");
            }

            if (String.IsNullOrEmpty(this.EndToken))
            {
                throw new AquilesCommandParameterException("EndToken must not be null or empty.");
            }
        }

        
    }
}
