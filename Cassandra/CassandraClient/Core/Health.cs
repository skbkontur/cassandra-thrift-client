using System.Threading;

namespace CassandraClient.Core
{
    public class Health
    {
        public double Value
        {
            get
            {
                return Interlocked.CompareExchange(ref val, 0, 0);
            }
            set
            {
                Interlocked.Exchange(ref val, value);
            }
        }
        private double val;
    }
}