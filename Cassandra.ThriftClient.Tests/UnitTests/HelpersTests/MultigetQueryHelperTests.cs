using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Apache.Cassandra;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Exceptions;
using SkbKontur.Cassandra.ThriftClient.Helpers;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Tests.UnitTests.HelpersTests
{
    public class MultigetQueryHelperTests : TestBase
    {
        [Test]
        public void TestPartialFetcher()
        {
            var result = DefaultMultigetQueryHelper.EnumerateAllKeysWithPartialFetcher(keys, ReturnFirstElementFetcherFactory(), silentLog);
            CollectionAssert.AreEquivalent(result.Select(item => (Key : item.Key, Value : item.Value)), keysWithValues);
        }

        [Test]
        public void TestFullFetcher()
        {
            var result = DefaultMultigetQueryHelper.EnumerateAllKeysWithPartialFetcher(keys, FullFetcherFactory(), silentLog);
            CollectionAssert.AreEquivalent(result.Select(item => (Key : item.Key, Value : item.Value)), keysWithValues);
        }

        [Test]
        public void TestFetcherThatShuffleRequestPrefix()
        {
            var result = DefaultMultigetQueryHelper.EnumerateAllKeysWithPartialFetcher(keys, ReversePrefixFetcherFactory(prefixLength : 7), silentLog);
            CollectionAssert.AreEquivalent(result.Select(item => (Key : item.Key, Value : item.Value)), keysWithValues);
        }

        [Test]
        public void TestFetcherThatStuck()
        {
            const string commandName = "test_command_name";
            const string keyspace = "test_keyspace_name";
            const string columnFamily = "test_column_family_name";
            const string consistencyLevel = "QUORUM";

            void FetchKeys() => new MultigetQueryHelper(commandName, keyspace, columnFamily, ConsistencyLevel.QUORUM)
                .EnumerateAllKeysWithPartialFetcher(keys, EmptyFetcherFactory(), silentLog);

            Assert.Throws(Is.TypeOf<CassandraClientInvalidResponseException>()
                            .And.Message.Contains(commandName)
                            .And.Message.Contains(keyspace)
                            .And.Message.Contains(columnFamily)
                            .And.Message.Contains(consistencyLevel), FetchKeys);
        }

        [Test]
        public void TestFetcherThatDoesNotReturnRequestPrefix()
        {
            var result = DefaultMultigetQueryHelper.EnumerateAllKeysWithPartialFetcher(keys, ReverseSuffixFetcherFactory(suffixLength : 7), silentLog);
            CollectionAssert.AreEquivalent(result.Select(item => (Key : item.Key, Value : item.Value)), keysWithValues);
        }

        private MultigetQueryHelper DefaultMultigetQueryHelper => new MultigetQueryHelper(string.Empty, string.Empty, string.Empty, null);

        private static Func<List<byte[]>, Dictionary<byte[], int>> ReturnFirstElementFetcherFactory()
        {
            return k => new Dictionary<byte[], int> {[k[0]] = k[0][0]};
        }

        private static Func<List<byte[]>, Dictionary<byte[], int>> FullFetcherFactory()
        {
            return k => k.Select(key => (Key : key, Value : (int)key[0])).ToDictionary(item => item.Key, item => item.Value);
        }

        private static Func<List<byte[]>, Dictionary<byte[], int>> EmptyFetcherFactory()
        {
            return _ => new Dictionary<byte[], int>();
        }

        private static Func<List<byte[]>, Dictionary<byte[], int>> ReversePrefixFetcherFactory(int prefixLength)
        {
            return k =>
                {
                    var reversedKeys = k.Take(prefixLength).ToList();
                    reversedKeys.Reverse();
                    return reversedKeys.Select(key => (Key : key, Value : (int)key[0])).ToDictionary(item => item.Key, item => item.Value);
                };
        }

        private static Func<List<byte[]>, Dictionary<byte[], int>> ReverseSuffixFetcherFactory(int suffixLength)
        {
            return k =>
                {
                    var reversedKeys = k.Skip(k.Count - suffixLength).ToList();
                    reversedKeys.Reverse();
                    return reversedKeys.Select(key => (Key : key, Value : (int)key[0])).ToDictionary(item => item.Key, item => item.Value);
                };
        }

        private static byte[] GetBytes(string s) => Encoding.UTF8.GetBytes(s);
        private static readonly ILog silentLog = new SilentLog();
        private static readonly List<byte[]> keys = Enumerable.Range(0, 100).Select(i => GetBytes(i.ToString())).ToList();
        private static readonly List<(byte[] Key, int Value)> keysWithValues = keys.Select(key => (Key : key, Value : (int)key[0])).ToList();
    }
}