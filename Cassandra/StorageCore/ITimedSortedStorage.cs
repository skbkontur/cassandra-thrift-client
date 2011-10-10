using System.Collections.Generic;

namespace SKBKontur.Cassandra.StorageCore
{
    public interface ITimedSortedStorage
    {
        void Append(string category, ulong ticks, string id);
        void AppendBatch(string category, IEnumerable<TimedStorageElement> elements);
        TimedStorageElement[] Get(string category, int count, TimedStorageElement greatThanElement = null);
    }
}