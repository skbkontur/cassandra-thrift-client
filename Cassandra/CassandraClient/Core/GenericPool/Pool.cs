using System;
using System.Collections.Concurrent;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Utils;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    public class Pool<T> : IDisposable where T : class, IDisposable
    {
        public Pool(Func<Pool<T>, T> itemFactory)
        {
            this.itemFactory = itemFactory;
        }

        public void Dispose()
        {
            var items = freeItems.Union(busyItems.Keys).ToArray();
            foreach(var item in items)
                item.Dispose();
        }

        public T Acquire()
        {
            T result;
            return TryAcquireExists(out result) ? result : AcquireNew();
        }

        public bool TryAcquireExists(out T result)
        {
            if(freeItems.TryPop(out result))
            {
                MarkItemAsBusy(result);
                return true;
            }
            return false;
        }

        public void Release(T item)
        {
            object dummy;
            if(!busyItems.TryRemove(item, out dummy))
                throw new FailedReleaseItemException(item.ToString());
            freeItems.Push(item);
        }

        public T AcquireNew()
        {
            var result = itemFactory(this);
            MarkItemAsBusy(result);
            return result;
        }

        public int TotalCount { get { return busyItems.Count + freeItems.Count; } }

        private void MarkItemAsBusy(T result)
        {
            if(!busyItems.TryAdd(result, new object()))
                throw new ItemInPoolCollisionException();
        }

        private readonly Func<Pool<T>, T> itemFactory;
        private readonly ConcurrentStack<T> freeItems = new ConcurrentStack<T>();
        private readonly ConcurrentDictionary<T, object> busyItems = new ConcurrentDictionary<T, object>(ObjectReferenceEqualityComparer<T>.Default);
    }
}