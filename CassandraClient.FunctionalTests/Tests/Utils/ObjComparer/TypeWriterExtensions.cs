using System;
using System.Xml;

namespace SKBKontur.Cassandra.FunctionalTests.Utils.ObjComparer
{
    public static class TypeWriterExtensions

    {
        public static void Write(this ITypeWriter typeWriter, Type type, object value, XmlWriter writer)
        {
            if(!typeWriter.TryWrite(type, value, writer))
                throw new InvalidOperationException(string.Format("Не удалось записать объект типа '{0}' ", type));
        }
    }
}