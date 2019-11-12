using System;
using System.IO;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Managed wrapper around arrow::io::RandomAccessFile that takes in a .NET Stream instance.
    /// </summary>
    public sealed class ManagedRandomAccessFile : RandomAccessFile
    {
        public ManagedRandomAccessFile(Stream stream)
            : this(stream, false)
        {
        }

        public ManagedRandomAccessFile(Stream stream, bool leaveOpen)
        {
            _stream = stream;
            _leaveOpen = leaveOpen;
            _read = Read;
            _close = Close;
            _getSize = GetSize;
            _tell = Tell;
            _seek = Seek;
            _closed = Closed;

            Handle = Create(_read, _close, _getSize, _tell, _seek, _closed);
        }

        private static ParquetHandle Create(
            ReadDelegate read,
            CloseDelegate close,
            GetSizeDelegate getSize,
            TellDelegate tell,
            SeekDelegate seek,
            ClosedDelegate closed)
        {
            ExceptionInfo.Check(ManagedRandomAccessFile_Create(read, close, getSize, tell, seek, closed, out var handle));
            return new ParquetHandle(handle, RandomAccessFile_Free);
        }

        private byte Read(long nbytes, IntPtr bytesRead, IntPtr dest, out string exception)
        {
            try
            {
#if NETSTANDARD20
                unsafe
                {
                    var read = Stream.Read(new Span<byte>(dest.ToPointer(), (int)nbytes));
                    Marshal.WriteInt64(bytes_read, read);
                }
#else
                var buffer = new byte[(int) nbytes];
                var read = _stream.Read(buffer, 0, (int) nbytes);
                Marshal.Copy(buffer, 0, dest, read);
                Marshal.WriteInt64(bytesRead, read);
#endif
                exception = null;
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
                if(!_leaveOpen)
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

        private byte GetSize(IntPtr size, out string exception)
        {
            try
            {
                Marshal.WriteInt64(size, _stream.Length);
                exception = null;
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
                exception = null;
                return 0;
            }
            catch (Exception error)
            {
                return HandleException(error, out exception);
            }
        }

        private byte Seek(long position, out string exception)
        {
            try
            {
                _stream.Position = position;
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
                return !_stream.CanRead;
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
        private static extern IntPtr ManagedRandomAccessFile_Create(
            ReadDelegate read,
            CloseDelegate close,
            GetSizeDelegate getSize,
            TellDelegate tell,
            SeekDelegate seek,
            ClosedDelegate closed,
            out IntPtr randomAccessFile);

        private delegate byte ReadDelegate(long nbyte, IntPtr bytesRead, IntPtr dest, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private delegate byte CloseDelegate([MarshalAs(UnmanagedType.LPStr)] out string exception);
        private delegate byte GetSizeDelegate(IntPtr size, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private delegate byte TellDelegate(IntPtr position, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private delegate byte SeekDelegate(long position, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private delegate bool ClosedDelegate();

        private readonly Stream _stream;
        private readonly bool _leaveOpen;

        // The lifetime of the delegates must match the lifetime of this class.
        // ReSharper disable PrivateFieldCanBeConvertedToLocalVariable
        private readonly ReadDelegate _read;
        private readonly CloseDelegate _close;
        private readonly GetSizeDelegate _getSize;
        private readonly TellDelegate _tell;
        private readonly SeekDelegate _seek;
        private readonly ClosedDelegate _closed;
        // ReSharper restore PrivateFieldCanBeConvertedToLocalVariable

        // The lifetime of the exception message must match the lifetime of this class.
        // ReSharper disable NotAccessedField.Local
        private string _exceptionMessage;
        // ReSharper restore NotAccessedField.Local
    }
}
