
using System;

namespace ParquetSharp.Test
{
    // tanguyf: 2018-05-30: For the moment VS does not abide the launchSettings.json nativeDebugging when running
    // the unit tests. As long as this is the case, provide our own Main().
    // https://github.com/dotnet/project-system/issues/3495
    // https://andrewlock.net/fixing-the-error-program-has-more-than-one-entry-point-defined-for-console-apps-containing-xunit-tests/
    public static class Program
    {
        public static int Main()
        {
            try
            {
                Console.WriteLine("Working directory: {0}", Environment.CurrentDirectory);

                TestColumnReader.TestHasNext();
                //TestLogicalTypeRoundtrip.TestReaderWriteTypes();
                TestPhysicalTypeRoundtrip.TestReaderWriteTypes();
                TestParquetFileReader.TestReadFileCreateByPython();

                return 0;
            }

            catch (Exception exception)
            {
                var colour = Console.ForegroundColor;

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: {0}", exception);
                Console.ForegroundColor = colour;
            }

            return 1;
        }
    }
}
