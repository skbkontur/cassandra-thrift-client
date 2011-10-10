using System.Net;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    public class ConnectionPoolKey
    {
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != typeof(ConnectionPoolKey)) return false;
            return Equals((ConnectionPoolKey)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((IpEndPoint != null ? IpEndPoint.GetHashCode() : 0) * 397) ^ (Keyspace != null ? Keyspace.GetHashCode() : 0);
            }
        }

        public IPEndPoint IpEndPoint { get; set; }
        public string Keyspace { get; set; }

        private bool Equals(ConnectionPoolKey other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(other.IpEndPoint, IpEndPoint) && Equals(other.Keyspace, Keyspace);
        }
    }
}