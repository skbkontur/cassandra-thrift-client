using Aquiles.Helpers.Encoders;
using Aquiles.Model;

using CassandraClient.Abstractions;

namespace CassandraClient.Helpers
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