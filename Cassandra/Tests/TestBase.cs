using System;
using System.Threading;

using NUnit.Framework;

using Rhino.Mocks;

namespace Cassandra.Tests
{
    [TestFixture]
    public abstract class TestBase
    {
        [SetUp]
        public virtual void SetUp()
        {
            MockRepository = new MockRepository();
        }

        [TearDown]
        public virtual void TearDown()
        {
            MockRepository.VerifyAll();
        }

        public static T GetStub<T>()
        {
            var mock = MockRepository.Stub<T>();
            mock.Replay();
            return mock;
        }

        public static void AssertEqualsFull<T>(T expected, T actual)
        {
            Assert.AreEqual(expected, actual, "actual:\n{0}\nexpected:\n{1}", actual, expected);
        }

        public static void RunMethodWithException<TE>(Action method) where TE : Exception
        {
            RunMethodWithException(method, (Action<TE>)null);
        }

        public static void RunMethodWithException<TE>(Action method, Action<TE> exceptionCheckDelegate)
            where TE : Exception
        {
            if(typeof(TE) == typeof(Exception) || typeof(TE) == typeof(AssertionException))
                Assert.Fail("использование типа {0} запрещено", typeof(TE));
            try
            {
                method();
            }
            catch(TE e)
            {
                if(e is ThreadAbortException)
                    Thread.ResetAbort();
                if(exceptionCheckDelegate != null)
                    exceptionCheckDelegate(e);
                return;
            }
            Assert.Fail("Method didn't thrown expected exception " + typeof(TE));
        }

        public static MockRepository MockRepository { get; private set; }

        protected static T GetMock<T>()
        {
            var mock = MockRepository.StrictMock<T>();
            mock.Replay();
            return mock;
        }

        protected static void RunMethodWithException<TE>(Action method, string expectedMessageSubstring)
            where TE : Exception
        {
            Assert.That(!String.IsNullOrEmpty(expectedMessageSubstring));
            RunMethodWithException<TE>(method, e => StringAssert.Contains(expectedMessageSubstring, e.Message));
        }

        protected static void VerifyAll()
        {
            MockRepository.VerifyAll();
            MockRepository.BackToRecordAll(BackToRecordOptions.None);
            MockRepository.ReplayAll();
        }

        protected static IDisposable Ordered()
        {
            return new MockeryOrdered();
        }

        private class MockeryOrdered : IDisposable
        {
            public MockeryOrdered()
            {
                ordered = MockRepository.Ordered();
            }

            public void Dispose()
            {
                ordered.Dispose();
                MockRepository.ReplayAll();
            }

            private readonly IDisposable ordered;
        }
    }
}