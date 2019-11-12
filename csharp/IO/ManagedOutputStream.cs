using System;
using System.IO;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Managed wrapper around arrow::io::OutputStream that takes in a .NET Stream instance.
    /// </summary>
    public sealed class ManagedOutputStream : OutputStream
    {
        public ManagedOutputStream(Stream stream)
            : this(stream, false)
        {
        }

        public ManagedOutputStream(Stream stream, bool leaveOpen)
        {
            _stream = stream;
            _leaveOpen = leaveOpen;
            _write = Write;
            _tell = Tell;
            _flush = Flush;
            _close = Close;
            _closed = Closed;

            Handle = Create(_write, _tell, _flush, _close, _closed);
        }

        private static ParquetHandle Create(
            WriteDelegate write,
            TellDelegate tell,
            FlushDelegate flush,
            CloseDelegate close,
            ClosedDelegate closed)
        {
            ExceptionInfo.Check(ManagedOutputStream_Create(write, tell, flush, close, closed, out var handle));
            return new ParquetHandle(handle, OutputStream_Free);
        }

        private byte Write(IntPtr src, long nbytes, out string exception)
        {
            try
            {
#if !NETSTANDARD20
                var buffer = new byte[(int) nbytes];
#endif

                while (nbytes > 0)
                {
                    var ibytes = (int) nbytes;

#if NETSTANDARD20
                    unsafe
                    {
                        _stream.Write(new Span<byte>(src.ToPointer(), ibytes));
                    }
#else
                    Marshal.Copy(src, buffer, 0, ibytes);
                    _stream.Write(buffer, 0, ibytes);
#endif

                    nbytes -= ibytes;
                    src = IntPtr.Add(src, ibytes);
                }

                exception = _exceptionMessage = null;
                return 0;
            }
            catch (Exception error)
            {
                return HandleException(error, out exception);
            }
        }

        private byte Tell(IntPtr position, out string exception)
        {
            try
            {
                Marshal.WriteInt64(position, _stream.Position);
                exception = _exceptionMessage = null;
                return 0;
            }
            catch (Exception error)
            {
                return HandleException(error, out exception);
            }
        }

        private byte Flush(out string exception)
        {
            try
            {
                _stream.Flush();
                exception = _exceptionMessage = null;
                return 0;
            }
            catch (Exception error)
            {
                return HandleException(error, out exception);
            }
        }

        private byte Close(out string exception)
        {
            try
            {
                if (!_leaveOpen)
                {
                    _stream.Close();
                }

                exception = null;
                return 0;
            }
            catch (Exception error)
            {
                return HandleException(error, out exception);
            }
        }

        private bool Closed()
        {
            try
            {
                return !_stream.CanWrite;
            }
            catch
            {
                return true;
            }
        }

        private byte HandleException(Exception error, out string exception)
        {
            if (error is OutOfMemoryException)
            {
                exception = _exceptionMessage = null;
                return 1;
            }
            if (error is IOException)
            {
                exception = _exceptionMessage = error.ToString();
                return 5;
            }

            exception = _exceptionMessage = error.ToString();
            return 9;
        }

        [DllImport(ParquetDll.Name)]
        private static extern IntPtr ManagedOutputStream_Create(
            WriteDelegate write,
            TellDelegate tell,
            FlushDelegate flush,
            CloseDelegate close,
            ClosedDelegate closed,
            out IntPtr outputStream);


        private delegate byte WriteDelegate(IntPtr buffer, long nbyte, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private delegate byte TellDelegate(IntPtr position, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private delegate byte FlushDelegate([MarshalAs(UnmanagedType.LPStr)] out string exception);
        private delegate byte CloseDelegate([MarshalAs(UnmanagedType.LPStr)] out string exception);
        private delegate bool ClosedDelegate();

        private readonly Stream _stream;
        private readonly bool _leaveOpen;

        // The lifetime of the delegates must match the lifetime of this class.
        // ReSharper disable PrivateFieldCanBeConvertedToLocalVariable
        private readonly WriteDelegate _write;
        private readonly TellDelegate _tell;
        private readonly FlushDelegate _flush;
        private readonly CloseDelegate _close;
        private readonly ClosedDelegate _closed;
        // ReSharper restore PrivateFieldCanBeConvertedToLocalVariable

        // The lifetime of the exception message must match the lifetime of this class.
        // ReSharper disable NotAccessedField.Local
        private string _exceptionMessage;
        // ReSharper restore NotAccessedField.Local
    }
}
