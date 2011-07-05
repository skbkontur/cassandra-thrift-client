using System.Collections.Generic;
using System.Linq;

namespace CassandraClient.Helpers
{
    public class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public int GetHashCode(byte[] obj)
        {
            return obj.Aggregate(0, (current, b) => current * 23 + (b + 1));
        }

        public bool Equals(byte[] x, byte[] y)
        {
            if(x.Length != y.Length) return false;
            return !x.Where((t, i) => t != y[i]).Any();
        }

        public static ByteArrayComparer SimpleComparer { get { return new ByteArrayComparer(); } }
    }
}