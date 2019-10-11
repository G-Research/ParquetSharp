using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Wrapper around arrow::io::OutputStream, implemented in C#
    /// </summary>
    public sealed class ManagedOutputStream : OutputStream
    {
        private System.IO.Stream Stream;

        private delegate byte WriteDelegate(IntPtr buffer, long nbyte, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private WriteDelegate _write;
        private delegate byte TellDelegate(IntPtr position, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private TellDelegate _tell;
        private delegate byte FlushDelegate([MarshalAs(UnmanagedType.LPStr)] out string exception);
        private FlushDelegate _flush;
        private delegate byte CloseDelegate([MarshalAs(UnmanagedType.LPStr)] out string exception);
        private CloseDelegate _close;
        private delegate bool ClosedDelegate();
        private ClosedDelegate _closed;

        public ManagedOutputStream(System.IO.Stream stream)
        {
            this.Stream = stream;

            this._write = (WriteDelegate)this.Write;
            this._tell = (TellDelegate)this.Tell;
            this._flush = (FlushDelegate)this.Flush;
            this._close = (CloseDelegate)this.Close;
            this._closed = (ClosedDelegate)this.Closed;

            ExceptionInfo.Check(ManagedOutputStream_Create(
                this._write, this._tell, this._flush, this._close, this._closed, out var handle));

            this.Handle = new ParquetHandle(handle, OutputStream.OutputStream_Free);
        }

        private byte Write(IntPtr src, long nbytes, out string exception)
        {
            try {
                #if !NETSTANDARD20
                byte[] buffer = new byte[(int)nbytes];
                #endif

                while (nbytes > 0)
                {
                    int ibytes = (int)nbytes;

                    #if NETSTANDARD20
                    unsafe
                    {
                        Stream.Write(new Span<byte>(src.ToPointer(), ibytes));
                    }
                    #else
                    Marshal.Copy(src, buffer, 0, ibytes);
                    Stream.Write(buffer, 0, ibytes);
                    #endif

                    nbytes -= ibytes;
                    src = IntPtr.Add(src, ibytes);
                }

                exception = null;
                return 0;
            } catch (OutOfMemoryException) {
                exception = null;
                return 1;
            } catch (Exception exc) {
                exception = exc.ToString();
                return 9;
            }
        }

        private byte Tell(IntPtr position, out string exception)
        {
            try {
                Marshal.WriteInt64(position, Stream.Position);
                exception = null;
                return 0;
            } catch (OutOfMemoryException) {
                exception = null;
                return 1;
            } catch (Exception exc) {
                exception = exc.ToString();
                return 9;
            }
        }

        private byte Flush(out string exception)
        {
            try {
                Stream.Flush();
                exception = null;
                return 0;
            } catch (OutOfMemoryException) {
                exception = null;
                return 1;
            } catch (Exception exc) {
                exception = exc.ToString();
                return 9;
            }
        }

        private byte Close(out string exception)
        {
            try {
                Stream.Close();
                exception = null;
                return 0;
            } catch (OutOfMemoryException) {
                exception = null;
                return 1;
            } catch (Exception exc) {
                exception = exc.ToString();
                return 9;
            }
        }

        private bool Closed()
        {
            try {
                return !Stream.CanWrite;
            } catch {
                return true;
            }
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ManagedOutputStream_Create(
            WriteDelegate write,
            TellDelegate tell,
            FlushDelegate flush,
            CloseDelegate close,
            ClosedDelegate closed,
             out IntPtr outputStream);
    }
}
