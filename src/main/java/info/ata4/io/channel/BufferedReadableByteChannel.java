/*
 ** 2015 February 22
 **
 ** The author disclaims copyright to this source code. In place of
 ** a legal notice, here is a blessing:
 **    May you do good and not evil.
 **    May you find forgiveness for yourself and forgive others.
 **    May you share freely, never taking more than you give.
 */
package info.ata4.io.channel;

import info.ata4.io.buffer.source.ReadableByteChannelSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 *
 * @author Nico Bergemann <barracuda415 at yahoo.de>
 */
public class BufferedReadableByteChannel
    extends BufferedChannel<ReadableByteChannel, ReadableByteChannelSource>
    implements ReadableByteChannel {
    
    public static final int DEFAULT_BUFFER_SIZE = 1 << 20; // 1 MiB

    public BufferedReadableByteChannel(ReadableByteChannel in, int bufferSize) {
        super(in, new ReadableByteChannelSource(ByteBuffer.allocateDirect(bufferSize), in));
    }
    
    public BufferedReadableByteChannel(ReadableByteChannel in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return buf.read(dst);
    }
}
