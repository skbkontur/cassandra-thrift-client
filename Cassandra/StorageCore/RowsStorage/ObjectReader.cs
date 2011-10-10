using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.StorageCore.RowsStorage
{
    public class ObjectReader : IObjectReader
    {
        public ObjectReader(IVersionReaderCollection versionReaderCollection)
        {
            this.versionReaderCollection = versionReaderCollection;
        }

        public bool TryReadObject<T>(Column[] columns, out T result) where T : class
        {
            columns = columns ?? new Column[0];
            Column[] specialColumns = GetSpecialColumns(columns);
            string version = GetVersion(specialColumns);
            IVersionReader versionReader = versionReaderCollection.GetVersionReader(version);
            return versionReader.TryReadObject(columns, specialColumns, out result);
        }

        private string GetVersion(Column[] specialColumns)
        {
            foreach(var specialColumn in specialColumns)
            {
                if(specialColumn.Name == SerializeToRowsStorageConstants.formatVersionColumnName)
                    return StringHelpers.BytesToString(specialColumn.Value);
            }
            return FormatVersions.version1;
        }

        private Column[] GetSpecialColumns(Column[] columns)
        {
            var result = new List<Column>();
            foreach(var column in columns)
            {
                if(SerializeToRowsStorageConstants.SpecialColumnNames.Contains(column.Name, StringComparer.OrdinalIgnoreCase))
                    result.Add(column);
            }
            return result.ToArray();
        }

        private readonly IVersionReaderCollection versionReaderCollection;
    }
}