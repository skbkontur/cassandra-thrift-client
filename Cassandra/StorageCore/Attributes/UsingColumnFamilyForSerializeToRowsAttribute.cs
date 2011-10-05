using System;

namespace StorageCore.Attributes
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public class UsingColumnFamilyForSerializeToRowsAttribute : UsingColumnFamilyForTypeAttribute
    {
        public UsingColumnFamilyForSerializeToRowsAttribute(string columnFamilyName, Type type)
            : base(columnFamilyName, type)
        {
        }
    }
}