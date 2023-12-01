using System;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;
using NUnit.Framework.Internal.Builders;

namespace ParquetSharp.Test
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    internal sealed class TestCaseGenericAttribute : TestCaseAttribute, ITestBuilder
    {
        public TestCaseGenericAttribute(params object[] arguments)
            : base(arguments)
        {
        }

        public Type[] TypeArguments { get; set; } = null!;

        IEnumerable<TestMethod> ITestBuilder.BuildFrom(IMethodInfo method, NUnit.Framework.Internal.Test? suite)
        {
            if (!method.IsGenericMethodDefinition)
                return base.BuildFrom(method, suite);

            if (TypeArguments == null || TypeArguments.Length != method.GetGenericArguments().Length)
            {
                var parms = new TestCaseParameters {RunState = RunState.NotRunnable};
                parms.Properties.Set("_SKIPREASON", $"{nameof(TypeArguments)} should have {method.GetGenericArguments().Length} elements");
                return new[] {new NUnitTestCaseBuilder().BuildTestMethod(method, suite, parms)};
            }

            var genMethod = method.MakeGenericMethod(TypeArguments);
            return base.BuildFrom(genMethod, suite);
        }
    }
}
