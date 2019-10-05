using System;
using System.Runtime.InteropServices;

namespace ParquetSharp.IO
{
    /// <summary>
    /// Wrapper around arrow::io::RandomAccessFile, implemented in C#
    /// </summary>
    public sealed class ManagedRandomAccessFile : RandomAccessFile
    {
        private System.IO.Stream Stream;

        private delegate byte ReadDelegate(long nbyte, IntPtr bytes_read, IntPtr dest, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private ReadDelegate _read;
        private delegate byte CloseDelegate([MarshalAs(UnmanagedType.LPStr)] out string exception);
        private CloseDelegate _close;
        private delegate byte GetSizeDelegate(IntPtr size, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private GetSizeDelegate _getSize;
        private delegate byte TellDelegate(IntPtr position, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private TellDelegate _tell;
        private delegate byte SeekDelegate(long position, [MarshalAs(UnmanagedType.LPStr)] out string exception);
        private SeekDelegate _seek;
        private delegate bool ClosedDelegate();
        private ClosedDelegate _closed;

        public ManagedRandomAccessFile(System.IO.Stream stream)
        {
            this.Stream = stream;

            this._read = (ReadDelegate)this.Read;
            this._close = (CloseDelegate)this.Close;
            this._getSize = (GetSizeDelegate)this.GetSize;
            this._tell = (TellDelegate)this.Tell;
            this._seek = (SeekDelegate)this.Seek;
            this._closed = (ClosedDelegate)this.Closed;

            ExceptionInfo.Check(ManagedRandomAccessFile_Create(
                this._read, this._close, this._getSize, this._tell, this._seek, this._closed, out var handle));

            this.Handle = new ParquetHandle(handle, RandomAccessFile.RandomAccessFile_Free);
        }

        private byte Read(long nbytes, IntPtr bytes_read, IntPtr dest, out string exception)
        {
            try {
                #if NETSTANDARD20
                unsafe
                {
                    var read = Stream.Read(new Span<byte>(dest.ToPointer(), (int)nbytes));
                    Marshal.WriteInt64(bytes_read, read);
                }
                #else
                byte[] buffer = new byte[(int)nbytes];
                var read = Stream.Read(buffer, 0, (int)nbytes);
                Marshal.Copy(buffer, 0, dest, read);
                Marshal.WriteInt64(bytes_read, read);
                #endif
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

        private byte GetSize(IntPtr size, out string exception)
        {
            try {
                Marshal.WriteInt64(size, Stream.Length);
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

        private byte Seek(long position, out string exception)
        {
            try {
                Stream.Position = position;
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
                return !Stream.CanRead;
            } catch {
                return true;
            }
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
    }
}
