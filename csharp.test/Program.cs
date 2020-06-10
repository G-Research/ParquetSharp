
using System;
using System.Runtime.ExceptionServices;

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

                AppDomain.CurrentDomain.UnhandledException += UncaughtExceptionHandler;

                //TestColumn.TestPrimitives();
                //TestParquetFileWriter.TestReadWriteParquetMultipleTasks();
                //TestColumnReader.TestHasNext();
                TestLogicalTypeRoundtrip.TestRoundTrip(128, 2401, 1331, useDictionaryEncoding: true);
                //TestPhysicalTypeRoundtrip.TestReaderWriteTypes();
                //TestParquetFileReader.TestReadFileCreateByPython();
                //TestParquetFileReader.TestFileHandleHasBeenReleased();
                //TestParquetFileWriter.TestWriteLongString();
                //TestManagedRandomAccessFile.TestWriteException();
                //TestAadPrefixVerifier.TestOwnership();
                //TestEncryption.TestNoMatchingKeyMetadata();
                //TestParquetFileWriter.TestDisposeExceptionSafety_ColumnWriter();

                // Ensure the finalizers are executed, so we can check whether they throw.
                GC.Collect();
                GC.WaitForPendingFinalizers();

                AppDomain.CurrentDomain.UnhandledException -= UncaughtExceptionHandler;

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

        [HandleProcessCorruptedStateExceptions]
        private static void UncaughtExceptionHandler(object sender, UnhandledExceptionEventArgs args)
        {
            Console.Error.WriteLine("FATAL: UncaughtExceptionHandler: {0}", args.ExceptionObject);
            Environment.Exit(1);
        }
    }
}
