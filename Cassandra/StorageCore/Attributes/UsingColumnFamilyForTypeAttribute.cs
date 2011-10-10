using System;

namespace SKBKontur.Cassandra.StorageCore.Attributes
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public abstract class UsingColumnFamilyForTypeAttribute : UsingColumnFamilyAttribute
    {
        protected UsingColumnFamilyForTypeAttribute(string columnFamilyName, Type type)
            : base(columnFamilyName)
        {
            Type = type;
        }

        public Type Type { get; private set; }
    }
}