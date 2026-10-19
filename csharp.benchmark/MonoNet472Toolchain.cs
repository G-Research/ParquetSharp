using System;
using System.Collections.Generic;
using System.Diagnostics;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Toolchains;
using BenchmarkDotNet.Toolchains.CsProj;
using BenchmarkDotNet.Toolchains.DotNetCli;
using BenchmarkDotNet.Toolchains.Parameters;
using BenchmarkDotNet.Toolchains.Results;

namespace ParquetSharp.Benchmark
{
    /// <summary>
    /// Builds the benchmarks for net472 with the dotnet CLI and runs them with Mono.
    /// BenchmarkDotNet cannot do this out of the box: CsProjClassicNetToolchain refuses to run
    /// outside Windows, and the stock executor passes anonymous pipe descriptors to the child
    /// process, which Mono's FileStream cannot open from a raw file descriptor. Launching the
    /// child without the pipe arguments makes it fall back to reporting results on standard
    /// output (the same protocol BenchmarkDotNet's WASM executor relies on).
    /// </summary>
    internal static class MonoNet472Toolchain
    {
        public static readonly IToolchain Instance = new Toolchain(
            "net472 (Mono)",
            new CsProjGenerator("net472", cliPath: null!, packagesPath: null!, runtimeFrameworkVersion: null!, isNetCore: false),
            new DotNetCliBuilder("net472"),
            new MonoConsoleExecutor());

        private sealed class MonoConsoleExecutor : IExecutor
        {
            public ExecuteResult Execute(ExecuteParameters executeParameters)
            {
                var exePath = executeParameters.BuildResult.ArtifactsPaths.ExecutablePath;
                var logger = executeParameters.Logger;

                var startInfo = new ProcessStartInfo
                {
                    FileName = "mono",
                    Arguments = $"\"{exePath}\" {executeParameters.BenchmarkId.ToArguments()}",
                    WorkingDirectory = executeParameters.BuildResult.ArtifactsPaths.BinariesDirectoryPath,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true,
                };

                var output = new List<string>();
                using var process = new Process { StartInfo = startInfo };
                process.OutputDataReceived += (_, e) =>
                {
                    if (e.Data != null)
                    {
                        lock (output)
                        {
                            output.Add(e.Data);
                        }
                        logger.WriteLine(e.Data);
                    }
                };

                logger.WriteLineInfo($"// Execute: {startInfo.FileName} {startInfo.Arguments} in {startInfo.WorkingDirectory}");
                process.Start();
                process.BeginOutputReadLine();

                // ponytail: fixed 30 minute cap instead of configurable timeouts; many times more
                // than any benchmark in this repository needs.
                if (process.WaitForExit((int)TimeSpan.FromMinutes(30).TotalMilliseconds))
                {
                    // The timed overload does not wait for asynchronous output to drain.
                    process.WaitForExit();
                }
                else
                {
                    logger.WriteLineError("// The Mono benchmark process did not exit within 30 minutes, killing it.");
                    process.Kill();
                    process.WaitForExit(5000);
                }

                var results = new List<string>();
                var prefixedLines = new List<string>();
                foreach (var line in output)
                {
                    (line.StartsWith("//", StringComparison.Ordinal) ? prefixedLines : results).Add(line);
                }

                return new ExecuteResult(
                    foundExecutable: true,
                    exitCode: process.HasExited ? process.ExitCode : (int?)null,
                    processId: process.Id,
                    results: results,
                    prefixedLines: prefixedLines,
                    standardOutput: output,
                    launchIndex: executeParameters.LaunchIndex);
            }
        }
    }
}
