using System;
using System.Text;
using System.Xml;

using Cassandra.Tests.ObjComparer;

using NUnit.Framework;

namespace Cassandra.Tests
{
    public static class ObjectComparer
    {
        public static void AssertEqualsTo<T>(this T actual, T expected)
        {
            var badXml = "<root></root>".ReformatXml();

            string expectedStr = expected.ObjectToString();
            Assert.AreNotEqual(expectedStr.ReformatXml(), badXml, "bug(expected)");
            string actualStr = actual.ObjectToString();
            Assert.AreNotEqual(actualStr.ReformatXml(), badXml, "bug(actual)");
            TestBase.AssertEqualsFull(expectedStr, actualStr);
        }

        public static string ObjectToString<T>(this T instance)
        {
            return ObjectToString(typeof(T), instance);
        }

        private static string ObjectToString(Type type, object instance)
        {
            if(type.IsInterface)
                throw new InvalidOperationException(string.Format("Cannot compare interface type={0}", type.Name));
            var builder = new StringBuilder();
            var writer = XmlWriter.Create(builder,
                                          new XmlWriterSettings {Indent = true, OmitXmlDeclaration = true});
            var objectWriter = new ObjectWriter(writer,
                                                new NodeProcessor(CompareTypeAs.Declared, CompareInterfaceAs.None));
            objectWriter.Write(type, instance);
            writer.Flush();

            return builder.ToString();
        }
    }
}