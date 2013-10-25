using System;
using System.Net;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    public class ConnectionPoolKey
    {
        public override bool Equals(object obj)
        {
            if(ReferenceEquals(null, obj)) return false;
            if(ReferenceEquals(this, obj)) return true;
            if(obj.GetType() != typeof(ConnectionPoolKey)) return false;
            return Equals((ConnectionPoolKey)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = (IpEndPoint != null ? IpEndPoint.GetHashCode() : 0);
                result = (result * 397) ^ (Keyspace != null ? Keyspace.GetHashCode() : 0);
                result = (result * 397) ^ IsFierce.GetHashCode();
                return result;
            }
        }

        public IPEndPoint IpEndPoint { get; set; }
        public string Keyspace { get; set; }
        public bool IsFierce { get; set; }

        private bool Equals(ConnectionPoolKey other)
        {
            if(ReferenceEquals(null, other)) return false;
            if(ReferenceEquals(this, other)) return true;
            return Equals(other.IpEndPoint, IpEndPoint) && Equals(other.Keyspace, Keyspace) && other.IsFierce.Equals(IsFierce);
        }

        public override string ToString()
        {
            return string.Format("EndPoint={0}; KeySpace={1}, IsFierce={2}", IpEndPoint, Keyspace, IsFierce);
        }
    }

    public class ConnectionKey : IEquatable<ConnectionKey>
    {
        public ConnectionKey(string keyspace, bool isFierce)
        {
            Keyspace = keyspace;
            ISFierce = isFierce;
        }

        public bool Equals(ConnectionKey other)
        {
            if(ReferenceEquals(null, other)) return false;
            if(ReferenceEquals(this, other)) return true;
            return string.Equals(Keyspace, other.Keyspace) && IsFierce.Equals(other.IsFierce);
        }

        public override bool Equals(object obj)
        {
            if(ReferenceEquals(null, obj)) return false;
            if(ReferenceEquals(this, obj)) return true;
            if(obj.GetType() != this.GetType()) return false;
            return Equals((ConnectionKey)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Keyspace != null ? Keyspace.GetHashCode() : 0) * 397) ^ IsFierce.GetHashCode();
            }
        }

        public string Keyspace { get; set; }
        public bool ISFierce { get; set; }
        public bool IsFierce { get; set; }
    }
}