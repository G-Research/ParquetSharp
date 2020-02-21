using System;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestAadPrefixVerifier
    {
        [Test]
        public static void TestOwnership()
        {
            // The FileDecryptionPropertiesBuilder and FileDecryptionProperties own a std::shared_ptr on our aad-prefix-verifier.
            // Assert that the partial transfer of ownership of the aad-prefix-verifier from C# to C++ is successful.

            var weakRef = AssertOwnership();

            GC.Collect();

            Assert.IsFalse(weakRef.IsAlive, "weak reference should not be alive anymore");
        }

        private static WeakReference AssertOwnership()
        {
            using var properties = CreateProperties();

            GC.Collect();

            // At this point C# has no reference to the key-receiver. And yet we can still get it back from C++.
            var aadPrefixVerifier = (TestVerifier) properties.AadPrefixVerifier;

            Assert.AreEqual("HelloWorld Exception!", aadPrefixVerifier.ExceptionMessage);

            // But after we return, both C# and C++ will lose all references and the aad-prefix-verifier should get GCed. 
            return new WeakReference(aadPrefixVerifier);
        }

        private static FileDecryptionProperties CreateProperties()
        {
            using var builder = new FileDecryptionPropertiesBuilder();
            builder.FooterKey(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
            builder.AadPrefixVerifier(new TestVerifier("HelloWorld Exception!"));
            return builder.Build();
        }

        private sealed class TestVerifier : AadPrefixVerifier
        {
            public TestVerifier(string exceptionMessage)
            {
                ExceptionMessage = exceptionMessage;
            }

            public readonly string ExceptionMessage;

            public override void Verify(string aadPrefix) => throw new ArgumentException(ExceptionMessage);
        }
    }
}