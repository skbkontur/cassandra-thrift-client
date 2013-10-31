using System;
using System.Collections;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal static class ToStringValueEnumExtensions
    {
        public static string ToStringValue(this Enum value)
        {
            var enumType = value.GetType();
            if(enumToString[enumType] == null)
            {
                lock(enumToString)
                {
                    if(enumToString[enumType] == null)
                        enumToString[enumType] = BuildEnumToString(enumType);
                }
            }
            var hashtable = (Hashtable)enumToString[enumType];
            var result = hashtable[value];
            if(result == null)
                throw new Exception(string.Format("The string value not found for enum value '{0}' of type '{1}'", value, enumType.Name));
            return (string)result;
        }

        public static T FromStringValue<T>(this string value)
        {
            var enumType = typeof(T);
            if(!enumType.IsEnum)
                throw new Exception(string.Format("The type '{0}' not enum", enumType.Name));
            if(stringToEnum[enumType] == null)
            {
                lock(stringToEnum)
                {
                    if(stringToEnum[enumType] == null)
                        stringToEnum[enumType] = BuildStringToEnum(enumType);
                }
            }
            var hashtable = (Hashtable)stringToEnum[enumType];
            var result = hashtable[value];
            if(result == null)
                throw new Exception(string.Format("The enum value of type '{0}' not found for string value '{1}'", enumType.Name, value));
            return (T)result;
        }

        private static Hashtable BuildEnumToString(Type enumType)
        {
            var result = new Hashtable();
            foreach(var value in Enum.GetValues(enumType))
            {
                var stringValue = GetStringValue((Enum)value);
                if(stringValue == null)
                    continue;
                result.Add(value, GetStringValue((Enum)value));
            }
            return result;
        }

        private static Hashtable BuildStringToEnum(Type enumType)
        {
            var result = new Hashtable();
            foreach(var value in Enum.GetValues(enumType))
            {
                var stringValue = GetStringValue((Enum)value);
                if(result.ContainsKey(stringValue))
                    throw new Exception(string.Format("The string '{0}' is the string value both for values '{1}' and '{2}'", stringValue, value, result[stringValue]));
                result.Add(stringValue, value);
            }
            return result;
        }

        private static string GetStringValue(Enum value)
        {
            var memberInfos = value.GetType().GetMember(value.ToString());
            if(memberInfos.Length > 0)
            {
                var attrs = memberInfos[0].GetCustomAttributes(typeof(StringValueAttribute), false);
                if(attrs.Length > 0)
                    return ((StringValueAttribute)attrs[0]).StringValue;
            }
            return null;
        }

        private static readonly Hashtable enumToString = new Hashtable();
        private static readonly Hashtable stringToEnum = new Hashtable();
    }
}