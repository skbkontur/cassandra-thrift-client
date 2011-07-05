using System;
using System.Collections.Generic;
using System.Reflection;

namespace Cassandra.Tests.ObjComparer
{
    public class TypeHelpers
    {
        public static List<FieldInfo> GetTypeInstanceFields(Type type)
        {
            FieldInfo[] fields = type.GetFields(BindingFlags.Public | BindingFlags.NonPublic
                                                | BindingFlags.SetField | BindingFlags.Instance | BindingFlags.Static);
            var result = new List<FieldInfo>();

            foreach(var info in fields)
            {
                if(!info.IsStatic && !info.IsLiteral)
                    result.Add(info);
            }
            return result;
        }

        public static List<FieldInfo> GetFields(Type type)
        {
            Type currentType = type;
            var result = new List<FieldInfo>();
            while(currentType != null)
            {
                result.AddRange(GetTypeInstanceFields(currentType));
                currentType = currentType.BaseType;
            }
            return result;
        }
    }
}