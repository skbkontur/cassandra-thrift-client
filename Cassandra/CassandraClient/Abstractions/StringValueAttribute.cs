using System;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    [AttributeUsage(AttributeTargets.Field, AllowMultiple = false)]
    internal sealed class StringValueAttribute : Attribute
    {
        public StringValueAttribute(string stringValue)
        {
            StringValue = stringValue;
        }

        public string StringValue { get; set; }
    }
}