﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Utils;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    public class Pool<T> : IDisposable where T : class, IDisposable, ILiveness
    {
        public Pool(Func<Pool<T>, T> itemFactory)
        {
            this.itemFactory = itemFactory;
        }

        public void Dispose()
        {
            var items = freeItems.Select(x => x.Item).Union(busyItems.Keys).ToArray();
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
            while(TryFreeItemPop(out result))
            {
                if(!result.IsAlive)
                {
                    result.Dispose();
                    continue;
                }
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
            freeItems.Push(new FreeItemInfo(item, DateTime.UtcNow));
        }

        public T AcquireNew()
        {
            var result = itemFactory(this);
            MarkItemAsBusy(result);
            return result;
        }

        public int RemoveIdleItems(TimeSpan minIdleTimeSpan)
        {
            unusedItemCollectorLock.EnterWriteLock();
            try
            {
                var tempStack = new Stack<FreeItemInfo>();
                var x = DateTime.UtcNow;
                FreeItemInfo item;
                var result = 0;
                while(freeItems.TryPop(out item))
                {
                    if(x - item.IdleTime >= minIdleTimeSpan)
                    {
                        result++;
                        item.Item.Dispose();
                        continue;
                    }
                    tempStack.Push(item);
                }
                while(tempStack.Count > 0)
                    freeItems.Push(tempStack.Pop());
                return result;
            }
            finally
            {
                unusedItemCollectorLock.ExitWriteLock();
            }
        }

        public int TotalCount { get { return FreeItemCount + BusyItemCount; } }
        public int FreeItemCount { get { return freeItems.Count; } }
        public int BusyItemCount { get { return busyItems.Count; } }

        private bool TryFreeItemPop(out T item)
        {
            unusedItemCollectorLock.EnterReadLock();
            try
            {
                FreeItemInfo freeItemInfo;
                var result = freeItems.TryPop(out freeItemInfo);
                item = freeItemInfo.Item;
                return result;
            }
            finally
            {
                unusedItemCollectorLock.ExitReadLock();
            }
        }

        private void MarkItemAsBusy(T result)
        {
            if(!busyItems.TryAdd(result, new object()))
                throw new ItemInPoolCollisionException();
        }

        private readonly ReaderWriterLockSlim unusedItemCollectorLock = new ReaderWriterLockSlim();

        private readonly Func<Pool<T>, T> itemFactory;
        private readonly ConcurrentStack<FreeItemInfo> freeItems = new ConcurrentStack<FreeItemInfo>();
        private readonly ConcurrentDictionary<T, object> busyItems = new ConcurrentDictionary<T, object>(ObjectReferenceEqualityComparer<T>.Default);

        private class FreeItemInfo
        {
            public FreeItemInfo(T item, DateTime idleTime)
            {
                Item = item;
                IdleTime = idleTime;
            }

            public T Item { get; private set; }
            public DateTime IdleTime { get; private set; }
        }
    }
}