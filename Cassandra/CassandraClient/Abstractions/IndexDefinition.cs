using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class IndexDefinition
    {
        public string Name { get; set; }
        public DataType ValidationClass { get; set; }
    }

    internal static class IndexDefinitionExtensions
    {
        public static ColumnDef ToCassandraColumnDef(this IndexDefinition indexDefinition)
        {
            if(indexDefinition == null)
                return null;
            return new ColumnDef
                {
                    Name = StringExtensions.StringToBytes(indexDefinition.Name),
                    Index_type = IndexType.KEYS,
                    Validation_class = indexDefinition.ValidationClass.ToStringValue()
                };
        }

        public static IndexDefinition FromCassandraColumnDef(this ColumnDef columnDef)
        {
            if(columnDef == null)
                return null;
            return new IndexDefinition
                {
                    Name = StringExtensions.BytesToString(columnDef.Name),
                    ValidationClass = columnDef.Validation_class.FromStringValue<DataType>()
                };
        }
    }
}