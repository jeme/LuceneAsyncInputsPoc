using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Store;

namespace LuceneAsyncInputsPoc.CustomDirectory;

public interface IAsyncIndexInput
{
    Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellation);
}

public class CustomSimpleFSDirectory : SimpleFSDirectory
{
    public CustomSimpleFSDirectory(DirectoryInfo path, LockFactory lockFactory) : base(path, lockFactory) { }
    public CustomSimpleFSDirectory(DirectoryInfo path) : base(path) { }
    public CustomSimpleFSDirectory(string path, LockFactory lockFactory) : base(path, lockFactory) { }
    public CustomSimpleFSDirectory(string path) : base(path) { }


    public override IndexInput OpenInput(string name, IOContext context)
    {
        EnsureOpen();
        string path = Path.Combine(Directory.FullName, name);
        FileStream raf = new (path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        return new CustomSimpleFSIndexInput($"SimpleFSIndexInput(path=\"{path}\")", raf, context);
    }

    protected class CustomSimpleFSIndexInput : SimpleFSIndexInput, IAsyncIndexInput
    {
        public CustomSimpleFSIndexInput(string resourceDesc, FileStream file, IOContext context) : base(resourceDesc, file, context)
        {
        }

        public CustomSimpleFSIndexInput(string resourceDesc, FileStream file, long off, long length, int bufferSize) : base(resourceDesc, file, off, length, bufferSize)
        {
        }

        public async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellation)
        {
            return await m_file.ReadAsync(buffer, offset, count, cancellation);
        }
    }
}
public class IndexInputStream : Stream
{
    private readonly IndexInput input;

    public IndexInputStream(IndexInput input)
    {
        this.input = input;
    }

    public override void Flush()
    {
        throw new InvalidOperationException("Cannot flush a readonly stream.");
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        switch (origin)
        {
            case SeekOrigin.Begin:
                Position = offset;
                break;
            case SeekOrigin.Current:
                Position += offset;
                break;
            case SeekOrigin.End:
                Position = Length - offset;
                break;
        }
        return Position;
    }

    public override void SetLength(long value)
    {
        throw new InvalidOperationException("Cannot change length of a readonly stream.");
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        int remaining = (int)(input.Length - input.Position);
        int readCount = Math.Min(remaining, count);
        input.ReadBytes(buffer, offset, readCount);
        return readCount;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (input is IAsyncIndexInput asyncInput)
        {
            return await asyncInput.ReadAsync(buffer, offset, count, cancellationToken);
        }
        return Read(buffer, offset, count);
    }


    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new InvalidCastException("Cannot write to a readonly stream.");
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => input.Length;

    public override long Position
    {
        get => input.Position;
        set => input.Seek(value);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            input.Dispose();
        }
        base.Dispose(disposing);
    }
}