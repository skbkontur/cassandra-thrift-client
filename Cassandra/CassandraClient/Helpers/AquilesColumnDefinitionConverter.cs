using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Encoders;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class AquilesColumnDefinitionConverter
    {
        public static AquilesColumnDefinition ToAquilesColumnDefinition(this IndexDefinition indexDefinition)
        {
            return new AquilesColumnDefinition
                {
                    IsIndex = true,
                    Name = ByteEncoderHelper.UTF8Encoder.ToByteArray(indexDefinition.Name),
                    ValidationClass = indexDefinition.ValidationClass.ToString(),
                };
        }

        public static IndexDefinition ToIndexDefinition(this AquilesColumnDefinition aquilesColumnDefinition)
        {
            return new IndexDefinition
                {
                    Name = ByteEncoderHelper.UTF8Encoder.FromByteArray(aquilesColumnDefinition.Name),
                    ValidationClass =
                        aquilesColumnDefinition.ValidationClass.Contains("LongType")
                            ? ValidationClass.LongType
                            : aquilesColumnDefinition.ValidationClass.Contains("UTF8Type")
                                  ? ValidationClass.UTF8Type
                                  : ValidationClass.Undefined
                };
        }
    }
}