using System;

namespace CassandraClient.StorageCore.Attributes
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public class UsingColumnFamilyForSerializeToBlobAttribute : UsingColumnFamilyForTypeAttribute
    {
        public UsingColumnFamilyForSerializeToBlobAttribute(string columnFamilyName, Type type)
            : base(columnFamilyName, type)
        {
        }
    }
}