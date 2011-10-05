using System;

namespace StorageCore.Attributes
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public class UsingColumnFamilyAttribute : Attribute
    {
        public UsingColumnFamilyAttribute(string columnFamilyName)
        {
            ColumnFamilyName = columnFamilyName;
        }

        public string ColumnFamilyName { get; private set; }
    }
}