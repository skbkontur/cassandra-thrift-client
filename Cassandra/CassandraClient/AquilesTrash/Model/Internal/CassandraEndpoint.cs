using System;
using System.Collections.Generic;

using System.Text;
using System.Globalization;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model.Internal
{
    internal class CassandraEndpoint
    {
        public CassandraEndpoint(String address, int port, int timeout)
        {
            this.Address = address;
            this.Port = port;
            this.Timeout = timeout;
        }

        public String Address
        {
            get;
            private set;
        }

        public int Port
        {
            get;
            private set;
        }

        public int Timeout
        {
            get;
            private set;
        }

        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "{0}:{1}-{2}", this.Address, this.Port, this.Timeout);
        }
    }
}
