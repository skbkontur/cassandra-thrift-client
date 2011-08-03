using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Text;

namespace Cassandra.Tests
{
    public static class CoreTestHelpers
    {
        public static string DumpCollection(NameValueCollection collection)
        {
            var list = new List<CollectionSlot>();
            if(collection != null)
            {
                for(int i = 0; i < collection.Count; ++i)
                    list.Add(new CollectionSlot {Key = collection.GetKey(i), Values = collection.GetValues(i)});
            }
            list.Sort((x, y) => String.Compare(x.Key, y.Key));
            var result = new StringBuilder();
            foreach(var slot in list)
                result.Append((slot + "\r\n"));
            return result.ToString();
        }

        #region Nested type: CollectionSlot

        private class CollectionSlot
        {
            public override string ToString()
            {
                var result = new StringBuilder();
                result.Append("{'" + Key + "':{");
                if(Values != null)
                {
                    Array.Sort(Values);
                    bool notFirst = false;
                    foreach(var value in Values)
                    {
                        if(notFirst) result.Append(", ");
                        notFirst = true;
                        result.Append("'" + value + "'");
                    }
                }
                result.Append("}}");
                return result.ToString();
            }

            public string Key;
            public string[] Values;
        }

        #endregion
    }
}