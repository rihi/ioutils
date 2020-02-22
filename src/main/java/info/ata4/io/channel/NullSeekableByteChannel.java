package info.ata4.io.channel;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class NullSeekableByteChannel implements SeekableByteChannel {

    private boolean closed = false;
    private long position;

    public NullSeekableByteChannel() {
        this(Long.MAX_VALUE / 2);
    }

    public NullSeekableByteChannel(long startPosition) {
        this.position = startPosition;
    }

    @Override
    public int read(ByteBuffer dst) {
        if (position == 0)
            return -1;

        int read = (int) Math.min(position, dst.remaining());
        dst.position(dst.position() + read);
        position -= read;
        return read;
    }

    @Override
    public int write(ByteBuffer src) {
        if (position == Long.MAX_VALUE)
            return -1;

        int written = (int) Math.min(Long.MAX_VALUE - position, src.remaining());
        src.position(src.position() + written);
        position += written;
        return written;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) {
        if (newPosition < 0) {
            throw new IllegalArgumentException("newPosition must be positive");
        }

        position = newPosition;
        return this;
    }

    @Override
    public long size() {
        return position;
    }

    @Override
    public SeekableByteChannel truncate(long size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be positive");
        }

        if (size < size()) {
            position = size;
        }

        return this;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() {
        closed = true;
    }
}
