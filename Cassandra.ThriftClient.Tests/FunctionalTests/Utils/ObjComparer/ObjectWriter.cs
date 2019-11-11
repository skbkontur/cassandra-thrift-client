using System;
using System.Xml;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer
{
    public class ObjectWriter
    {
        public ObjectWriter(XmlWriter writer, INodeProcessor nodeProcessor)
        {
            this.writer = writer;
            this.nodeProcessor = nodeProcessor;
        }

        public void Write<T>(T value)
        {
            Write(typeof(T), value);
        }

        public void Write(Type type, object value)
        {
            Write(type, value, "root");
        }

        private void Write(Type type, object value, string name)
        {
            writer.WriteStartElement(name);
            DoWrite(type, value);
            writer.WriteEndElement();
        }

        private void DoWrite(Type type, object value)
        {
            if (IsBadType(type))
                throw new NotSupportedException("Не поддерживается тип " + type);

            if (TryWriteNullValue(value)) return;
            if (TryWriteNullableTypeValue(type, value)) return;
            if (TryWriteSimpleTypeValue(type, value)) return;
            if (TryWriteKnownTypeValue(type, value)) return;
            if (TryWriteArrayTypeValue(type, value)) return;
            WriteComplexTypeValue(type, value);
        }

        private static bool TryWriteKnownTypeValue(Type type, object value)
        {
            if (value == null) return false;
            return false;
        }

        private static bool IsBadType(Type type)
        {
            return false;
        }

        private void WriteComplexTypeValue(Type type, object value)
        {
            foreach (var fieldInfo in TypeHelpers.GetFields(type))
            {
                if (fieldInfo.FieldType.IsInterface)
                    continue;
                if (nodeProcessor.TryProcess(value, fieldInfo, out var typeToSerialize, out var fieldValue))
                    Write(typeToSerialize, fieldValue, FieldNameToTagName(fieldInfo.Name));
            }
        }

        private static string FieldNameToTagName(string name)
        {
            if (name.EndsWith(">k__BackingField"))
                return name.Substring(1, name.IndexOf('>') - 1);
            return name;
        }

        private bool TryWriteArrayTypeValue(Type type, object value)
        {
            if (!type.IsArray) return false;
            writer.WriteAttributeString("type", "array");
            var array = (Array)value;
            if (array.Rank > 1) throw new NotSupportedException("array with rank > 1");
            var elementType = type.GetElementType();
            for (var i = 0; i < array.Length; ++i)
            {
                var arrayItem = array.GetValue(i);
                Write(elementType, arrayItem, "item");
            }
            return true;
        }

        private bool TryWriteSimpleTypeValue(Type type, object value)
        {
            var result = SimpleTypeWriter.TryWrite(type, value);
            if (result != null)
            {
                writer.WriteValue(result);
                return true;
            }
            return false;
        }

        private bool TryWriteNullableTypeValue(Type type, object value)
        {
            if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nullable<>))
                return false;
            var getMethodHasValue = type.GetProperty("HasValue").GetGetMethod();
            var hasValue = (bool)getMethodHasValue.Invoke(value, new object[0]);
            if (!hasValue)
                WriteNull();
            else
            {
                var getMethodValue = type.GetProperty("Value").GetGetMethod();
                var nullableValue = getMethodValue.Invoke(value, new object[0]);
                DoWrite(type.GetGenericArguments()[0], nullableValue);
            }
            return true;
        }

        private bool TryWriteNullValue(object value)
        {
            if (value == null)
            {
                WriteNull();
                return true;
            }
            return false;
        }

        private void WriteNull()
        {
            writer.WriteAttributeString("type", "null");
        }

        private readonly INodeProcessor nodeProcessor;
        private readonly XmlWriter writer;
    }
}