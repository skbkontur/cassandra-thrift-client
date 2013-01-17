using System;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class IndexDefinition
    {
        public string Name { get; set; }
        public ValidationClass ValidationClass { get; set; }
    }

    internal static class IndexDefinitionExtensions
    {
        public static ColumnDef ToCassandraColumnDef(this IndexDefinition indexDefinition)
        {
            if(indexDefinition == null)
                return null;
            return new ColumnDef
                {
                    Name = StringHelpers.StringToBytes(indexDefinition.Name),
                    Index_type = IndexType.KEYS,
                    Validation_class = indexDefinition.ValidationClass.ToString()
                };
        }

        public static IndexDefinition FromCassandraColumnDef(this ColumnDef columnDef)
        {
            if(columnDef == null)
                return null;
            return new IndexDefinition
                {
                    Name = StringHelpers.BytesToString(columnDef.Name),
                    ValidationClass =
                        columnDef.Validation_class.IndexOf("LongType", StringComparison.OrdinalIgnoreCase) != -1
                            ? ValidationClass.LongType
                            : (columnDef.Validation_class.IndexOf("UTF8Type", StringComparison.OrdinalIgnoreCase) != -1)
                                  ? ValidationClass.UTF8Type
                                  : ValidationClass.Undefined
                };
        }
    }
}