using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    internal class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] x, byte[] y)
        {
            if (x == null && y == null) return true;
            if (x == null || y == null) return false;
            if (x.Length != y.Length) return false;
            for (var i = 0; i < x.Length; i++)
                if (x[i] != y[i]) return false;
            return true;
        }

        public int GetHashCode(byte[] obj)
        {
            return obj.Aggregate(0, (current, b) => current + (current * 197 + b));
        }

        public static ByteArrayEqualityComparer Instance => simpleComparer ?? (simpleComparer = new ByteArrayEqualityComparer());

        [ThreadStatic]
        private static ByteArrayEqualityComparer simpleComparer;
    }
}