using System;
using System.Reflection;
using System.Xml;

namespace Cassandra.Tests.ObjComparer
{
    public class ObjectWriter
    {
        public ObjectWriter(XmlWriter writer, INodeProcessor nodeProcessor)
        {
            this.writer = writer;
            this.nodeProcessor = nodeProcessor;
            simpleTypeWriter = new SimpleTypeWriter();
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
            if(IsBadType(type))
                throw new NotSupportedException("Не поддерживается тип " + type);

            if(TryWriteNullValue(value)) return;
            if(TryWriteNullableTypeValue(type, value)) return;
            if(TryWriteSimpleTypeValue(type, value)) return;
            if(TryWriteKnownTypeValue(type, value)) return;
            if(TryWriteArrayTypeValue(type, value)) return;
            WriteComplexTypeValue(type, value);
        }

        private bool TryWriteKnownTypeValue(Type type, object value)
        {
            if(value == null) return false;
            //if()
            return false;
        }

        private bool IsBadType(Type type)
        {
            return false; //type == typeof(object);
        }

        private void WriteComplexTypeValue(Type type, object value)
        {
            foreach(var fieldInfo in TypeHelpers.GetFields(type))
            {
                if(fieldInfo.FieldType.IsInterface)
                    continue;
                object fieldValue;
                Type typeToSerialize;
                if(nodeProcessor.TryProcess(value, fieldInfo, out typeToSerialize, out fieldValue))
                    Write(typeToSerialize, fieldValue, FieldNameToTagName(fieldInfo.Name));
            }
        }

        private static string FieldNameToTagName(string name)
        {
            if(name.EndsWith(">k__BackingField"))
                return name.Substring(1, name.IndexOf('>') - 1);
            return name;
        }

        private bool TryWriteArrayTypeValue(Type type, object value)
        {
            if(!type.IsArray) return false;
            writer.WriteAttributeString("type", "array");
            var array = (Array)value;
            if(array.Rank > 1) throw new NotSupportedException("array with rank > 1");
            Type elementType = type.GetElementType();
            for(int i = 0; i < array.Length; ++i)
            {
                object arrayItem = array.GetValue(i);
                Write(elementType, arrayItem, "item");
            }
            return true;
        }

        private bool TryWriteSimpleTypeValue(Type type, object value)
        {
            string result = simpleTypeWriter.TryWrite(type, value);
            if(result != null)
            {
                writer.WriteValue(result);
                return true;
            }
            return false;
        }

        private bool TryWriteNullableTypeValue(Type type, object value)
        {
            if(!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(Nullable<>))
                return false;
            MethodInfo getMethodHasValue = type.GetProperty("HasValue").GetGetMethod();
            var hasValue = (bool)getMethodHasValue.Invoke(value, new object[0]);
            if(!hasValue)
                WriteNull();
            else
            {
                MethodInfo getMethodValue = type.GetProperty("Value").GetGetMethod();
                object nullableValue = getMethodValue.Invoke(value, new object[0]);
                DoWrite(type.GetGenericArguments()[0], nullableValue);
            }
            return true;
        }

        private bool TryWriteNullValue(object value)
        {
            if(value == null)
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
        private readonly SimpleTypeWriter simpleTypeWriter;
        private readonly XmlWriter writer;
    }
}