using System;
using System.Threading;

using NUnit.Framework;

using Rhino.Mocks;

namespace Cassandra.ThriftClient.Tests.UnitTests
{
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

        private static void RunMethodWithException<TE>(Action method, Action<TE> exceptionCheckDelegate)
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
                exceptionCheckDelegate?.Invoke(e);
                return;
            }
            Assert.Fail("Method didn't thrown expected exception " + typeof(TE));
        }
    }
}