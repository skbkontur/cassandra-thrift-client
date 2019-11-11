using System;
using System.Xml;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer
{
    public static class TypeWriterExtensions
    {
        public static void Write(this ITypeWriter typeWriter, Type type, object value, XmlWriter writer)
        {
            if (!typeWriter.TryWrite(type, value, writer))
                throw new InvalidOperationException($"Не удалось записать объект типа '{type}' ");
        }
    }
}