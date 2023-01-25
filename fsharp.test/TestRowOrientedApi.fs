namespace ParquetSharp.Test.FSharp

open System
open System.IO
open NUnit.Framework
open FsUnit
open ParquetSharp.RowOriented

module TestRowOrientedApi =

    [<Struct>]
    type internal Record =
        {
            Field : int
        }

    [<Test>]
    let TestWritingInternalRecord () =
        let dir = Path.Combine(Path.GetTempPath (), Guid.NewGuid().ToString ()) |> Directory.CreateDirectory
        try
            let path = Path.Combine (dir.FullName, "f.parquet")

            using (ParquetFile.CreateRowWriter<Record> path) ( fun writer ->
                let r = { Field = 1 }
                writer.WriteRow r
            )

            use reader = ParquetFile.CreateRowReader<Record> path
            let got =
                seq {
                    for i in 0 .. reader.FileMetaData.NumRowGroups-1 do
                        for r in reader.ReadRows i do
                            yield r
                } |> Seq.toList

            got |> Seq.length |> should equal 1
        finally
            dir.Delete true
