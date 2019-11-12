using System;
using System.Text;
using System.Xml;

using NUnit.Framework;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer
{
    public static class ObjectComparer
    {
        public static void AssertEqualsTo<T>(this T actual, T expected)
        {
            var badXml = "<root></root>".ReformatXml();

            var expectedStr = expected.ObjectToString();
            Assert.AreNotEqual(expectedStr.ReformatXml(), badXml, "bug(expected)");
            var actualStr = actual.ObjectToString();
            Assert.AreNotEqual(actualStr.ReformatXml(), badXml, "bug(actual)");
            Assert.AreEqual(expectedStr, actualStr, "actual:\n{0}\nexpected:\n{1}", actualStr, expectedStr);
        }

        private static string ObjectToString<T>(this T instance)
        {
            return ObjectToString(typeof(T), instance);
        }

        private static string ObjectToString(Type type, object instance)
        {
            if (type.IsInterface)
                throw new InvalidOperationException($"Cannot compare interface type={type.Name}");
            var builder = new StringBuilder();
            var writer = XmlWriter.Create(builder, new XmlWriterSettings {Indent = true, OmitXmlDeclaration = true});
            var objectWriter = new ObjectWriter(writer, new NodeProcessor(CompareTypeAs.Declared, CompareInterfaceAs.None));
            objectWriter.Write(type, instance);
            writer.Flush();

            return builder.ToString();
        }
    }
}