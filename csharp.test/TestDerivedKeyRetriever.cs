using System;
using NUnit.Framework;

namespace ParquetSharp.Test
{
    [TestFixture]
    internal static class TestDerivedKeyRetriever
    {
        [Test]
        public static void TestOwnership()
        {
            // The FileDecryptionPropertiesBuilder and FileDecryptionProperties own a std::shared_ptr on our key-retriever.
            // Assert that the partial transfer of ownership of the key-retriever from C# to C++ is successful.

            var weakRef = AssertOwnership();

            GC.Collect();

            Assert.IsFalse(weakRef.IsAlive, "weak reference should not be alive anymore");
        }

        private static WeakReference AssertOwnership()
        {
            using var properties = CreateProperties();

            GC.Collect();

            // At this point C# has no reference to the key-receiver. And yet we can still get it back from C++.
            var keyReceiver = properties.KeyRetriever;

            Assert.AreEqual("HelloWorld\0 Key!", System.Text.Encoding.ASCII.GetString(keyReceiver.GetKey("not-used")));

            // But after we return, both C# and C++ will lose all references and the key-receiver should get GCed. 
            return new WeakReference(keyReceiver);
        }

        private static FileDecryptionProperties CreateProperties()
        {
            using var builder = new FileDecryptionPropertiesBuilder();
            builder.KeyRetriever(new TestRetriever("HelloWorld\0 Key!"));
            return builder.Build();
        }

        private sealed class TestRetriever : DecryptionKeyRetriever
        {
            public TestRetriever(string key)
            {
                _key = key;
            }

            public override byte[] GetKey(string keyMetadata) => System.Text.Encoding.ASCII.GetBytes(_key);

            private readonly string _key;
        }
    }
}
