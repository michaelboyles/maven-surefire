package org.apache.maven.plugin.surefire.extensions;

import org.apache.maven.plugin.surefire.extensions.EventConsumerThread.Memento;
import org.apache.maven.plugin.surefire.extensions.EventConsumerThread.Segment;
import org.apache.maven.plugin.surefire.log.api.ConsoleLogger;
import org.apache.maven.surefire.api.booter.ForkedProcessEventType;
import org.apache.maven.surefire.api.event.Event;
import org.apache.maven.surefire.extensions.EventHandler;
import org.apache.maven.surefire.extensions.ForkNodeArguments;
import org.apache.maven.surefire.extensions.util.CountdownCloseable;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.CharsetDecoder;
import java.util.Map;

import static java.lang.Math.min;
import static java.nio.charset.CodingErrorAction.REPLACE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.maven.plugin.surefire.extensions.EventConsumerThread.mapEventTypes;
import static org.apache.maven.surefire.api.booter.Constants.DEFAULT_STREAM_ENCODING;
import static org.fest.assertions.Assertions.assertThat;
import static org.powermock.reflect.Whitebox.invokeMethod;

/**
 * The performance of "get( Integer )" is 13.5 nano seconds on i5/2.6GHz:
 * <pre>
 *     {@code
 *     TreeMap<Integer, ForkedProcessEventType> map = new TreeMap<>();
 *     map.get( hash );
 *     }
 * </pre>
 *
 * <br> The performance of getting event type by Segment is 33.7 nano seconds:
 * <pre>
 *     {@code
 *     Map<Segment, ForkedProcessEventType> map = new HashMap<>();
 *     byte[] array = ForkedProcessEventType.BOOTERCODE_STDOUT.getOpcode().getBytes( UTF_8 );
 *     map.get( new Segment( array, 0, array.length ) );
 *     }
 * </pre>
 *
 * <br> The performance of decoder:
 * <pre>
 *     {@code
 *     CharsetDecoder decoder = STREAM_ENCODING.newDecoder()
 *             .onMalformedInput( REPLACE )
 *             .onUnmappableCharacter( REPLACE );
 *     ByteBuffer buffer = ByteBuffer.wrap( ForkedProcessEventType.BOOTERCODE_STDOUT.getOpcode().getBytes( UTF_8 ) );
 *     CharBuffer chars = CharBuffer.allocate( 100 );
 *     decoder.reset().decode( buffer, chars, true );
 *
 *     String s = chars.flip().toString(); // 37 nanos = CharsetDecoder + toString
 *
 *     buffer.clear();
 *     chars.clear();
 *
 *     ForkedProcessEventType.byOpcode( s ); // 65 nanos = CharsetDecoder + toString + byOpcode
 *     }
 * </pre>
 *
 * <br> The performance of decoding 100 bytes via CharacterDecoder - 71 nano seconds:
 * <pre>
 *     {@code
 *     decoder.reset()
 *         .decode( buffer, chars, true ); // CharsetDecoder 71 nanos
 *     chars.flip().toString(); // CharsetDecoder + toString = 91 nanos
 *     }
 * </pre>
 *
 * <br> The performance of a pure string creation (instead of decoder) - 31.5 nano seconds:
 * <pre>
 *     {@code
 *     byte[] b = {};
 *     new String( b, UTF_8 );
 *     }
 * </pre>
 *
 * <br> The performance of CharsetDecoder with empty ByteBuffer:
 * <pre>
 *     {@code
 *     CharsetDecoder + ByteBuffer.allocate( 0 ) makes 11.5 nanos
 *     CharsetDecoder + ByteBuffer.allocate( 0 ) + toString() makes 16.1 nanos
 *     }
 * </pre>
 */
public class EventConsumerThreadTest
{
    private static final CountdownCloseable COUNTDOWN_CLOSEABLE = new CountdownCloseable( new MockCloseable(), 0 );

    private static final String PATTERN1 =
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    private static final String PATTERN2 = "€ab©c";

    private static final byte[] PATTERN2_BYTES =
        new byte[]{(byte) -30, (byte) -126, (byte) -84, 'a', 'b', (byte) 0xc2, (byte) 0xa9, 'c'};

    @Test
    public void shouldDecodeHappyCase() throws Exception
    {
        CharsetDecoder decoder = UTF_8.newDecoder()
            .onMalformedInput( REPLACE )
            .onUnmappableCharacter( REPLACE );
        ByteBuffer input = ByteBuffer.allocate( 1024 );
        input.put( PATTERN2_BYTES )
            .flip();
        int bytesToDecode = PATTERN2_BYTES.length;
        CharBuffer output = CharBuffer.allocate( 1024 );
        int readBytes =
            invokeMethod( EventConsumerThread.class, "decode", decoder, input, output, bytesToDecode, true, 0 );

        assertThat( readBytes )
            .isEqualTo( bytesToDecode );

        assertThat( output.flip().toString() )
            .isEqualTo( PATTERN2 );
    }

    @Test
    public void shouldDecodeShifted() throws Exception
    {
        CharsetDecoder decoder = UTF_8.newDecoder()
            .onMalformedInput( REPLACE )
            .onUnmappableCharacter( REPLACE );
        ByteBuffer input = ByteBuffer.allocate( 1024 );
        input.put( PATTERN1.getBytes( UTF_8 ) )
            .put( 90, (byte) 'A' )
            .put( 91, (byte) 'B' )
            .put( 92, (byte) 'C' )
            .position( 90 );
        CharBuffer output = CharBuffer.allocate( 1024 );
        int readBytes =
            invokeMethod( EventConsumerThread.class, "decode", decoder, input, output, 2, true, 0 );

        assertThat( readBytes )
            .isEqualTo( 2 );

        assertThat( output.flip().toString() )
            .isEqualTo( "AB" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotDecode() throws Exception
    {
        CharsetDecoder decoder = UTF_8.newDecoder();
        ByteBuffer input = ByteBuffer.allocate( 100 );
        int bytesToDecode = 101;
        CharBuffer output = CharBuffer.allocate( 1000 );
        invokeMethod( EventConsumerThread.class, "decode", decoder, input, output, bytesToDecode, true, 0 );
    }

    @Test( expected = EOFException.class )
    public void shouldNotReadString() throws Exception
    {
        Channel channel = new Channel( PATTERN1.getBytes(), PATTERN1.length() );
        channel.read( ByteBuffer.allocate( 100 ) );

        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        memento.bb.position( 0 ).limit( 0 );
        thread.readString( memento, 10 );
    }

    @Test
    public void shouldReadString() throws Exception
    {
        Channel channel = new Channel( PATTERN1.getBytes(), PATTERN1.length() );

        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        memento.bb.position( 0 ).limit( 0 );
        String s = thread.readString( memento, 10 );
        assertThat( s )
            .isEqualTo( "0123456789" );
    }

    @Test
    public void shouldReadStringShiftedBuffer() throws Exception
    {
        StringBuilder s = new StringBuilder( 1100 );
        for ( int i = 0; i < 11; i++ )
        {
            s.append( PATTERN1 );
        }

        Channel channel = new Channel( s.toString().getBytes( UTF_8 ), s.length() );

        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        // whatever position will be compacted to 0
        memento.bb.position( 974 ).limit( 974 );
        assertThat( thread.readString( memento, PATTERN1.length() + 3 ) )
            .isEqualTo( PATTERN1 + "012" );
    }

    @Test
    public void shouldReadStringShiftedInput() throws Exception
    {
        StringBuilder s = new StringBuilder( 1100 );
        for ( int i = 0; i < 11; i++ )
        {
            s.append( PATTERN1 );
        }

        Channel channel = new Channel( s.toString().getBytes( UTF_8 ), s.length() );
        channel.read( ByteBuffer.allocate( 997 ) );

        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        memento.bb.limit( 0 );
        assertThat( thread.readString( memento, PATTERN1.length() ) )
            .isEqualTo( "789" + PATTERN1.substring( 0, 97 ) );
    }

    @Test
    public void shouldReadMultipleStringsAndShiftedInput() throws Exception
    {
        StringBuilder s = new StringBuilder( 5000 );

        for ( int i = 0; i < 50; i++ )
        {
            s.append( PATTERN1 );
        }

        Channel channel = new Channel( s.toString().getBytes( UTF_8 ), s.length() );
        channel.read( ByteBuffer.allocate( 1997 ) );

        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        // whatever position will be compacted to 0
        memento.bb.position( 974 ).limit( 974 );

        StringBuilder expected = new StringBuilder( "789" );
        for ( int i = 0; i < 11; i++ )
        {
            expected.append( PATTERN1 );
        }
        expected.setLength( 1100 );
        assertThat( thread.readString( memento, 1100 ) )
            .isEqualTo( expected.toString() );
    }

    @Test
    public void shouldDecode3BytesEncodedSymbol() throws Exception
    {
        byte[] encodedSymbol = new byte[] {(byte) -30, (byte) -126, (byte) -84};
        int countSymbols = 1024;
        byte[] input = new byte[encodedSymbol.length * countSymbols];
        for ( int i = 0; i < countSymbols; i++ )
        {
            System.arraycopy( encodedSymbol, 0, input, encodedSymbol.length * i, encodedSymbol.length );
        }

        Channel channel = new Channel( input, 2 );
        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );
        Memento memento = thread.new Memento();
        memento.bb.position( 0 ).limit( 0 );
        String decodedOutput = thread.readString( memento, input.length );

        assertThat( decodedOutput )
            .isEqualTo( new String( input, 0, input.length, UTF_8 ) );
    }

    @Test
    public void shouldDecode100Bytes()
    {
        CharsetDecoder decoder = DEFAULT_STREAM_ENCODING.newDecoder()
            .onMalformedInput( REPLACE )
            .onUnmappableCharacter( REPLACE );
        // empty stream: CharsetDecoder + ByteBuffer.allocate( 0 ) makes 11.5 nanos
        // empty stream: CharsetDecoder + ByteBuffer.allocate( 0 ) + toString() makes 16.1 nanos
        ByteBuffer buffer = ByteBuffer.wrap( PATTERN1.getBytes( UTF_8 ) );
        CharBuffer chars = CharBuffer.allocate( 100 );
        /* uncomment this section for a proper measurement of the exec time
        TimeUnit.SECONDS.sleep( 2 );
        System.gc();
        TimeUnit.SECONDS.sleep( 5 );
        */
        String s = null;
        long l1 = System.currentTimeMillis();
        for ( int i = 0; i < 10_000_000; i++ )
        {
            decoder.reset()
                .decode( buffer, chars, true ); // CharsetDecoder 71 nanos
            s = chars.flip().toString(); // CharsetDecoder + toString = 91 nanos
            buffer.clear();
            chars.clear();
        }
        long l2 = System.currentTimeMillis();
        System.out.println( "decoded 100 bytes within " + ( l2 - l1 ) );
        assertThat( s )
            .isEqualTo( PATTERN1 );
    }

    @Test
    public void shouldReadEventType() throws Exception
    {
        Map<Segment, ForkedProcessEventType> eventTypes = mapEventTypes();
        assertThat( eventTypes )
            .hasSize( ForkedProcessEventType.values().length );

        byte[] stream = ":maven-surefire-event:\u000E:std-out-stream:".getBytes( UTF_8 );
        Channel channel = new Channel( stream, 1 );
        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        memento.bb.position( 0 ).limit( 0 );
        memento.setCharset( UTF_8 );

        ForkedProcessEventType eventType = thread.readEventType( eventTypes, memento );
        assertThat( eventType )
            .isEqualTo( ForkedProcessEventType.BOOTERCODE_STDOUT );
    }

    @Test( expected = EOFException.class )
    public void shouldEventTypeReachedEndOfStream() throws Exception
    {
        Map<Segment, ForkedProcessEventType> eventTypes = mapEventTypes();
        assertThat( eventTypes )
            .hasSize( ForkedProcessEventType.values().length );

        byte[] stream = ":maven-surefire-event:\u000E:xxx".getBytes( UTF_8 );
        Channel channel = new Channel( stream, 1 );
        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        memento.bb.position( 0 ).limit( 0 );
        memento.setCharset( UTF_8 );
        thread.readEventType( eventTypes, memento );
    }

    @Test
    public void shouldReadEmptyString() throws Exception
    {
        byte[] stream = "\u0000\u0000\u0000\u0000::".getBytes( UTF_8 );
        Channel channel = new Channel( stream, 1 );
        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        memento.bb.position( 0 ).limit( 0 );
        memento.setCharset( UTF_8 );

        assertThat( thread.readString( memento ) )
            .isEmpty();
    }

    @Test
    public void shouldReadNullString() throws Exception
    {
        byte[] stream = "\u0000\u0000\u0000\u0001:\u0000:".getBytes( UTF_8 );
        Channel channel = new Channel( stream, 1 );
        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        memento.bb.position( 0 ).limit( 0 );
        memento.setCharset( UTF_8 );

        assertThat( thread.readString( memento ) )
            .isNull();
    }

    @Test
    public void shouldReadSingleCharString() throws Exception
    {
        byte[] stream = "\u0000\u0000\u0000\u0001:A:".getBytes( UTF_8 );
        Channel channel = new Channel( stream, 1 );
        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        memento.bb.position( 0 ).limit( 0 );
        memento.setCharset( UTF_8 );

        assertThat( thread.readString( memento ) )
            .isEqualTo( "A" );
    }

    @Test
    public void shouldReadThreeCharactersString() throws Exception
    {
        byte[] stream = "\u0000\u0000\u0000\u0003:ABC:".getBytes( UTF_8 );
        Channel channel = new Channel( stream, 1 );
        EventConsumerThread thread = new EventConsumerThread( "t", channel,
            new MockEventHandler<Event>(), COUNTDOWN_CLOSEABLE, new MockForkNodeArguments() );

        Memento memento = thread.new Memento();
        memento.bb.position( 0 ).limit( 0 );
        memento.setCharset( UTF_8 );

        assertThat( thread.readString( memento ) )
            .isEqualTo( "ABC" );
    }

    private static class Channel implements ReadableByteChannel
    {
        private final byte[] bytes;
        private final int chunkSize;
        private int i;

        public Channel( byte[] bytes, int chunkSize )
        {
            this.bytes = bytes;
            this.chunkSize = chunkSize;
        }

        @Override
        public int read( ByteBuffer dst )
        {
            if ( i == bytes.length )
            {
                return -1;
            }
            else if ( dst.hasRemaining() )
            {
                int length = min( min( chunkSize, bytes.length - i ), dst.remaining() ) ;
                dst.put( bytes, i, length );
                i += length;
                return length;
            }
            else
            {
                return 0;
            }
        }

        @Override
        public boolean isOpen()
        {
            return false;
        }

        @Override
        public void close()
        {
        }
    }

    private static class MockCloseable implements Closeable
    {
        @Override
        public void close()
        {
        }
    }

    private static class MockEventHandler<T> implements EventHandler<T>
    {
        @Override
        public void handleEvent( @Nonnull T event )
        {
        }
    }

    private static class MockForkNodeArguments implements ForkNodeArguments
    {
        @Nonnull
        @Override
        public String getSessionId()
        {
            return null;
        }

        @Override
        public int getForkChannelId()
        {
            return 0;
        }

        @Nonnull
        @Override
        public File dumpStreamText( @Nonnull String text )
        {
            return null;
        }

        @Nonnull
        @Override
        public File dumpStreamException( Throwable t )
        {
            return null;
        }

        @Override
        public void logWarningAtEnd( @Nonnull String text )
        {
        }

        @Nonnull
        @Override
        public ConsoleLogger getConsoleLogger()
        {
            return null;
        }
    }
}
