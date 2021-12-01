using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer
{
    public static class TypeHelpers
    {
        private static List<FieldInfo> GetTypeInstanceFields(Type type)
        {
            const BindingFlags fieldFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.SetField | BindingFlags.Instance | BindingFlags.Static;

            return type
                   .GetFields(fieldFlags)
                   .Where(info => !info.IsStatic && !info.IsLiteral)
                   .ToList();
        }

        public static List<FieldInfo> GetFields(Type type)
        {
            var currentType = type;
            var result = new List<FieldInfo>();
            while (currentType != null)
            {
                result.AddRange(GetTypeInstanceFields(currentType));
                currentType = currentType.BaseType;
            }
            return result;
        }
    }
}