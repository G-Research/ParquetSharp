namespace ParquetSharp.Test.FSharp

open System
open System.IO
open NUnit.Framework
open FsUnit
open ParquetSharp.RowOriented

module TestRowOrientedApi =

    type PublicRecord =
        {
            Field : int
        }

    type PublicRecordWithMappedField =
        {
            [<MapToColumn("field")>]
            Field : int
        }

    type internal InternalRecord =
        {
            Field : int
        }

    type internal InternalRecordWithMappedField =
        {
            [<MapToColumn("field")>]
            Field : int
        }

    [<Struct>]
    type internal InternalStructRecordWithMappedField =
        {
            [<MapToColumn("field")>]
            Field : int
        }

    let TestWritingRecord<'T> record verify =
        let dir = Path.Combine(Path.GetTempPath (), Guid.NewGuid().ToString ()) |> Directory.CreateDirectory
        try
            let path = Path.Combine (dir.FullName, "test.parquet")

            using (ParquetFile.CreateRowWriter<'T> path) ( fun writer ->
                writer.WriteRow record
            )

            use reader = ParquetFile.CreateRowReader<'T> path
            let got =
                seq {
                    for i in 0 .. reader.FileMetaData.NumRowGroups-1 do
                        for r in reader.ReadRows i do
                            yield r
                } |> Seq.toList

            got |> Seq.length |> should equal 1
            got |> Seq.head |> verify
        finally
            dir.Delete true

    [<Test>]
    let TestErrorWritingInternalRecord () =
        let dir = Path.Combine(Path.GetTempPath (), Guid.NewGuid().ToString ()) |> Directory.CreateDirectory
        try
            let path = Path.Combine (dir.FullName, "test.parquet")
            let expectedMessage = ("Type 'ParquetSharp.Test.FSharp.TestRowOrientedApi+InternalRecord' does not have " +
                "any public fields or properties to map to Parquet columns, or any private fields or properties " +
                "annotated with 'MapToColumnAttribute' (Parameter 'type')")
            (fun () -> ParquetFile.CreateRowWriter<InternalRecord> path |> ignore)
                |> should (throwWithMessage expectedMessage) typeof<System.ArgumentException>
        finally
            dir.Delete true

    [<Test>]
    let TestWritingPublicRecord () =
        TestWritingRecord<PublicRecord> { Field = 1 } ( fun readField -> readField.Field |> should equal 1)

    [<Test>]
    let TestWritingPublicRecordWithMappedField () =
        TestWritingRecord<PublicRecordWithMappedField> { Field = 1 } ( fun readField -> readField.Field |> should equal 1)

    [<Test>]
    let TestWritingInternalRecordWithMappedField () =
        TestWritingRecord<InternalRecordWithMappedField> { Field = 1 } ( fun readField -> readField.Field |> should equal 1)

    [<Test>]
    let TestWritingInternalStructRecordWithMappedField () =
        TestWritingRecord<InternalStructRecordWithMappedField> { Field = 1 } ( fun readField -> readField.Field |> should equal 1)
