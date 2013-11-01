using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.Net;

namespace Cassandra.Tests.ObjComparer
{
    public class SimpleTypeWriter
    {
        public static string TryWrite(Type type, object value)
        {
            if(type.IsEnum || type.IsPrimitive)
                return value.ToString();
            Func<object, string> func;
            if(!serializers.TryGetValue(type, out func))
                return null;
            return func(value);
        }

        private static string ObjToString(object o)
        {
            return o.ToString();
        }

        private static string NameValueCollectionToString(object o)
        {
            return CoreTestHelpers.DumpCollection((NameValueCollection)o);
        }

        private static readonly Dictionary<Type, Func<object, string>> serializers =
            new Dictionary<Type, Func<object, string>>
                {
                    {typeof(string), o => (string)o},
                    {typeof(Guid), ObjToString},
                    {typeof(NameValueCollection), NameValueCollectionToString},
                    {typeof(IPEndPoint), ObjToString},
                    {
                        typeof(DateTime), o =>
                            {
                                var t = (DateTime)o;
                                return t.Ticks.ToString(CultureInfo.InvariantCulture);
                            }
                    },
                };
    }
}