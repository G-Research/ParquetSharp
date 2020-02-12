﻿using System;
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
            using (var properties = CreateProperties())
            {
                GC.Collect();

                // At this point C# has no reference to the key-receiver. And yet we can still get it back from C++.
                var aadPrefixVerifier = properties.AadPrefixVerifier;

                Assert.AreEqual("HelloWorld Exception!", aadPrefixVerifier.Verify("not-used"));

                // But after we return, both C# and C++ will lose all references and the aad-prefix-verifier should get GCed. 
                return new WeakReference(aadPrefixVerifier);
            }
        }

        private static FileDecryptionProperties CreateProperties()
        {
            using (var builder = new FileDecryptionPropertiesBuilder())
            {
                builder.FooterKey("bogus-footer-key");
                builder.AadPrefixVerifier(new TestVerifier("HelloWorld Exception!"));
                return builder.Build();
            }
        }

        private sealed class TestVerifier : AadPrefixVerifier
        {
            public TestVerifier(string exception)
            {
                _exception = exception;
            }

            public override string Verify(string aadPrefix) => _exception;

            private readonly string _exception;
        }
    }
}