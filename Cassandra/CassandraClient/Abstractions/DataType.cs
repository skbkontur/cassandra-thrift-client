namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public enum DataType
    {
        [StringValue("org.apache.cassandra.db.marshal.UTF8Type")]
        UTF8Type,

        [StringValue("org.apache.cassandra.db.marshal.BytesType")]
        BytesType,

        [StringValue("org.apache.cassandra.db.marshal.AsciiType")]
        AsciiType,

        [StringValue("org.apache.cassandra.db.marshal.IntegerType")]
        IntegerType,

        [StringValue("org.apache.cassandra.db.marshal.Int32Type")]
        Int32Type,

        [StringValue("org.apache.cassandra.db.marshal.LongType")]
        LongType,

        [StringValue("org.apache.cassandra.db.marshal.UUIDType")]
        UUIDType,

        [StringValue("org.apache.cassandra.db.marshal.TimeUUIDType")]
        TimeUUIDType,

        [StringValue("org.apache.cassandra.db.marshal.DateType")]
        DateType,

        [StringValue("org.apache.cassandra.db.marshal.BooleanType")]
        BooleanType,

        [StringValue("org.apache.cassandra.db.marshal.FloatType")]
        FloatType,

        [StringValue("org.apache.cassandra.db.marshal.DoubleType")]
        DoubleType,

        [StringValue("org.apache.cassandra.db.marshal.DecimalType")]
        DecimalType,

        [StringValue("org.apache.cassandra.db.marshal.CounterColumnType")]
        CounterColumnType,

        [StringValue("org.apache.cassandra.db.marshal.CompositeType")]
        CompositeType
    }
}