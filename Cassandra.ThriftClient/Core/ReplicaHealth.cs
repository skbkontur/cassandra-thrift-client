using System.Threading;

namespace SkbKontur.Cassandra.ThriftClient.Core
{
    internal class ReplicaHealth<TReplicaKey>
    {
        public ReplicaHealth(TReplicaKey replicaKey, double initialHealth)
        {
            ReplicaKey = replicaKey;
            val = initialHealth;
        }

        public TReplicaKey ReplicaKey { get; }

        public double Value { get => Interlocked.CompareExchange(ref val, 0, 0); set => Interlocked.Exchange(ref val, value); }
        private double val;
    }
}