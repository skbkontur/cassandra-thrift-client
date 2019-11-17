using System;
using System.Collections.Generic;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.ThriftClient.Connections
{
    public interface ITimeBasedColumnFamilyConnection
    {
        void BatchInsert([NotNull] List<Tuple<string, List<TimeBasedColumn>>> data);

        [CanBeNull]
        TimeBasedColumn TryGetColumn([NotNull] string key, [NotNull] TimeGuid columnName);

        [NotNull]
        TimeBasedColumn[] GetRange([NotNull] string key, [CanBeNull] TimeGuid exclusiveStartColumnName, [CanBeNull] TimeGuid inclusiveEndColumnName, int take, bool reversed);
    }
}