using System;
using System.Threading;

using Moq;

using NUnit.Framework;

namespace Cassandra.ThriftClient.Tests.UnitTests
{
    public abstract class TestBase
    {
        [SetUp]
        public virtual void SetUp()
        {
            MockRepository = new MockRepository(MockBehavior.Strict);
        }

        [TearDown]
        public virtual void TearDown()
        {
            MockRepository.Verify();
        }

        private static MockRepository MockRepository { get; set; }

        protected static Mock<T> GetMock<T>() where T : class
        {
            return MockRepository.Create<T>();
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
            if (typeof(TE) == typeof(Exception) || typeof(TE) == typeof(AssertionException))
                Assert.Fail("использование типа {0} запрещено", typeof(TE));
            try
            {
                method();
            }
            catch (TE e)
            {
                if (e is ThreadAbortException)
                    Thread.ResetAbort();
                exceptionCheckDelegate?.Invoke(e);
                return;
            }
            Assert.Fail("Method didn't thrown expected exception " + typeof(TE));
        }
    }
}