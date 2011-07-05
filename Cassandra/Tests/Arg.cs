using System;

using Rhino.Mocks;
using Rhino.Mocks.Constraints;

namespace Cassandra.Tests
{
    public static class ARG
    {
        public static T TypeOf<T>()
        {
            return Arg<T>.Is.Anything;
        }

        public static T AreSame<T>(T obj)
        {
            return Arg<T>.Is.Same(obj);
        }

        public static OutRefArgDummy<T> Out<T>(T outValue)
        {
            return Arg<T>.Out(outValue);
        }

        public static T EqualsTo<T>(T obj)
        {
            return Arg<T>.Matches(Is.Matching<T>(actual => actual.ObjectToString().Equals(obj.ObjectToString())));
        }

        public static T MatchTo<T>(Func<T, bool> matcher)
        {
            return Arg<T>.Matches(Is.Matching<T>(obj => matcher(obj)));
        }

        public static T Equals<T>(T obj)
        {
            return Arg<T>.Is.Equal(obj);
        }
    }
}