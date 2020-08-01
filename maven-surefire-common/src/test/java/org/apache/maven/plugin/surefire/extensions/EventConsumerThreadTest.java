package org.apache.maven.plugin.surefire.extensions;

import org.apache.maven.plugin.surefire.extensions.EventConsumerThread.Memento;
import org.apache.maven.plugin.surefire.log.api.ConsoleLogger;
import org.apache.maven.surefire.api.booter.ForkedProcessEventType;
import org.apache.maven.surefire.extensions.EventHandler;
import org.apache.maven.surefire.extensions.ForkNodeArguments;
import org.apache.maven.surefire.extensions.util.CountdownCloseable;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.CodingErrorAction.REPLACE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.maven.plugin.surefire.extensions.EventConsumerThreadTest.StreamReadStatus.EOF;
import static org.apache.maven.plugin.surefire.extensions.EventConsumerThreadTest.StreamReadStatus.OVERFLOW;
import static org.apache.maven.plugin.surefire.extensions.EventConsumerThreadTest.StreamReadStatus.UNDERFLOW;
import static org.apache.maven.surefire.api.booter.Constants.STREAM_ENCODING;
import static org.fest.assertions.Assertions.assertThat;
//import static org.mockito.Mockito.mock;

//@RunWith( Parameterized.class )
public class EventConsumerThreadTest
{
    @Parameters
    public static Iterable<Object> channels()
    {
        return Arrays.asList( (Object) complexEncodings() );
    }

    @Parameter
    public Channel channel;

    private static Channel complexEncodings()
    {
        byte[] bytes = new byte[]{(byte) -30, (byte) -126, (byte) -84, 'a', 'b', (byte) 0xc2, (byte) 0xa9, 'c'};
        bytes = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789".getBytes( UTF_8 );
        return new Channel( bytes, 1 );
    }

    private static Channel complexEncodings( int chunkSize )
    {
        byte[] bytes = new byte[]{(byte) -30, (byte) -126, (byte) -84, 'a', 'b', (byte) 0xc2, (byte) 0xa9, 'c'};
        bytes = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789".getBytes( UTF_8 );
        return new Channel( bytes, chunkSize );
    }

    @Test
    public void test5() throws IOException, InterruptedException
    {
        /*final CharsetDecoder decoder = STREAM_ENCODING.newDecoder()
            .onMalformedInput( REPLACE )
            .onUnmappableCharacter( REPLACE );
        final CharBuffer chars = CharBuffer.allocate(5);

        final int totalBytes = 7;

        int countDecodedBytes = 0;
        int countReadBytes = 0;
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[4]);

        buffer.clear();

        int positionChars = chars.position();
        int startPosition;
        List<String> strings = new ArrayList<>();
        do
        {
            startPosition = buffer.position();
            buffer.limit( startPosition );
            read( buffer, totalBytes - countReadBytes );
            decoder.decode(buffer, chars, countDecodedBytes >= totalBytes );
            final boolean hasDecodedNewBytes = chars.position() != positionChars;
            if ( hasDecodedNewBytes )
            {
                countDecodedBytes += buffer.position() - startPosition;
                positionChars = chars.position();
            }
            countReadBytes += buffer.limit() - startPosition;
            buffer.compact();
            strings.add( chars.flip().toString() );
            chars.clear();
        }
        while ( countReadBytes < totalBytes );
        decoder.reset();

        String s = toString( strings );*/
        channel = complexEncodings(100);

        Closeable c = new Closeable()
        {
            @Override
            public void close() throws IOException
            {

            }
        };

        EventHandler eh = new EventHandler()
        {
            @Override
            public void handleEvent( @Nonnull Object event )
            {

            }
        };

        ForkNodeArguments forkNodeArguments = new ForkNodeArguments()
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
        };

        CountdownCloseable closeable = new CountdownCloseable( c, 0 );
        EventConsumerThread thread = new EventConsumerThread( "t", channel, eh, closeable,
            forkNodeArguments );
        Memento memento = new Memento();
        TimeUnit.SECONDS.sleep( 2 );
        System.gc();
        TimeUnit.SECONDS.sleep( 5 );
        long l1 = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            //memento.cb.clear();
            memento.bb.position(0);
            memento.bb.limit(memento.bb.capacity());
            memento.bytesCounter = 100;//7
            memento.data.clear();
            channel.reset();
            thread.readString( memento );
        }

        long l2 = System.currentTimeMillis();
        System.out.println(l2 - l1);
        /*assertThat( s )
            .isEqualTo( "€ab©" );*/
    }

    @Test
    public void test6() throws InterruptedException
    {
        CharsetDecoder decoder = STREAM_ENCODING.newDecoder()
            .onMalformedInput( REPLACE )
            .onUnmappableCharacter( REPLACE );
        // CharsetDecoder + ByteBuffer.allocate( 0 ) makes 11.5 nanos
        // CharsetDecoder + ByteBuffer.allocate( 0 ) + toString() makes 16.1 nanos
        ByteBuffer buffer = ByteBuffer.wrap( "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789".getBytes( UTF_8 ) );
        CharBuffer chars = CharBuffer.allocate( 100 );
        TimeUnit.SECONDS.sleep( 2 );
        System.gc();
        TimeUnit.SECONDS.sleep( 5 );
        long l1 = System.currentTimeMillis();
        for ( int i = 0; i < 10_000_000; i++ )
        {
            decoder
                .reset()
                .decode( buffer, chars, true ); // CharsetDecoder 71 nanos
            chars.flip().toString();//CharsetDecoder + toString = 91 nanos
            buffer.clear();
            chars.clear();
        }
        long l2 = System.currentTimeMillis();
        System.out.println(l2 - l1);
    }

    @Test
    public void test7() throws Exception {
        byte[] b = {};
        TimeUnit.SECONDS.sleep( 2 );
        System.gc();
        TimeUnit.SECONDS.sleep( 5 );
        String s = null;
        long l1 = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            s = new String(b, UTF_8);
        }

        long l2 = System.currentTimeMillis();
        System.out.println(l2 - l1);
        System.out.println(s);
    }

    @Test
    public void test8() {
        ForkedProcessEventType[] enums = ForkedProcessEventType.values();
        SortedSet<Integer> s = new TreeSet<>();
        for (ForkedProcessEventType e : enums) {
            if (s.contains( e.getOpcode().length() )) {
                System.out.println("obsahuje "  + e + " s dlzkou " + e.getOpcode().length() + " s has codom " + e.getOpcode().hashCode());
            }
            s.add( e.getOpcode().length() );
        }
    }

    @Test
    public void test9() throws InterruptedException
    {
        ForkedProcessEventType[] enums = ForkedProcessEventType.values();
        TreeMap<Integer, ForkedProcessEventType> map = new TreeMap<>();
        for (ForkedProcessEventType e : enums) {
            map.put( e.getOpcode().hashCode(), e );
        }
        TimeUnit.SECONDS.sleep( 2 );
        System.gc();
        TimeUnit.SECONDS.sleep( 5 );
        ForkedProcessEventType s = null;
        int hash = ForkedProcessEventType.BOOTERCODE_STDOUT.hashCode();
        long l1 = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            s = map.get( hash );
        }
        long l2 = System.currentTimeMillis();
        System.out.println(l2 - l1);
        System.out.println(s);
    }

    @Test
    public void test10() throws InterruptedException
    {
        ForkedProcessEventType[] enums = ForkedProcessEventType.values();
        Map<Segment, ForkedProcessEventType> map = new HashMap<>();
        for (ForkedProcessEventType e : enums) {
            byte[] array = e.getOpcode().getBytes();
            map.put( new Segment( array, 0, array.length ), e );
        }
        TimeUnit.SECONDS.sleep( 2 );
        System.gc();
        TimeUnit.SECONDS.sleep( 5 );
        ForkedProcessEventType s = null;
        byte[] array = ForkedProcessEventType.BOOTERCODE_STDOUT.getOpcode().getBytes();
        Segment segment = new Segment( array, 0, array.length );
        long l1 = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            s = map.get( new Segment( array, 0, array.length ) ); // 33.7 nanos
        }
        long l2 = System.currentTimeMillis();
        System.out.println(l2 - l1);
        System.out.println(s);
    }

    @Test
    public void test11() throws InterruptedException
    {
        CharsetDecoder decoder = STREAM_ENCODING.newDecoder()
            .onMalformedInput( REPLACE )
            .onUnmappableCharacter( REPLACE );
        ByteBuffer buffer = ByteBuffer.wrap( ForkedProcessEventType.BOOTERCODE_STDOUT.getOpcode().getBytes( UTF_8 ) );
        CharBuffer chars = CharBuffer.allocate( 100 );
        TimeUnit.SECONDS.sleep( 2 );
        System.gc();
        TimeUnit.SECONDS.sleep( 5 );
        long l1 = System.currentTimeMillis();
        for ( int i = 0; i < 10_000_000; i++ )
        {
            decoder
                .reset()
                .decode( buffer, chars, true );
            String s = chars.flip().toString(); // 37 nanos = CharsetDecoder + toString
            buffer.clear();
            chars.clear();
            ForkedProcessEventType.byOpcode( s ); // 65 nanos = CharsetDecoder + toString + byOpcode
        }
        long l2 = System.currentTimeMillis();
        System.out.println(l2 - l1);
    }

    @Test
    public void test12() throws InterruptedException
    {
        StringBuilder builder = new StringBuilder( 256 );
        TimeUnit.SECONDS.sleep( 2 );
        System.gc();
        TimeUnit.SECONDS.sleep( 5 );
        byte[] array = ForkedProcessEventType.BOOTERCODE_STDOUT.getOpcode().getBytes();
        long l1 = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            builder.setLength( 0 );
            for ( byte b : array )
            {
                char c = (char) b;
                if ( c == ':' ) break;
                builder.append( c );
            }
        }
        long l2 = System.currentTimeMillis();
        System.out.println(l2 - l1);
        System.out.println( builder.toString() );
    }

    private static class Segment {
        private final byte[] array;
        private final int fromIndex;
        private final int length;
        private final int hashCode;

        public Segment( byte[] array, int fromIndex, int length )
        {
            this.array = array;
            this.fromIndex = fromIndex;
            this.length = length;

            int hashCode = 0;
            int i = fromIndex;
            for (int loops = length >> 1; loops-- != 0; )
            {
                hashCode = 31 * hashCode + array[i++];
                hashCode = 31 * hashCode + array[i++];
            }
            this.hashCode = i == fromIndex + length ? hashCode : 31 * hashCode + array[i];
        }

        @Override
        public int hashCode()
        {
            return hashCode;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( !( obj instanceof Segment ) )
            {
                return false;
            }

            Segment that = (Segment) obj;
            if ( that.length != length )
            {
                return false;
            }

            for ( int i = 0; i < length; i++ )
            {
                if ( that.array[that.fromIndex + i] != array[fromIndex + i] )
                {
                    return false;
                }
            }
            return true;
        }
    }


    private static String toString( List<String> strings )
    {
        if ( strings.size() == 1 )
        {
            return strings.get( 0 );
        }
        StringBuilder concatenated = new StringBuilder();
        for ( String s : strings )
        {
            concatenated.append( s );
        }
        return concatenated.toString();
    }

    private StreamReadStatus read( ByteBuffer buffer, int recommendedCount ) throws IOException
    {
        if ( buffer.remaining() >= recommendedCount && buffer.position() != 0 )
        {
            return OVERFLOW;
        }
        else
        {
            int mark = buffer.position();
            buffer.position( buffer.limit() );
            buffer.limit( buffer.capacity() );
            boolean isEnd = false;
            while ( !isEnd && buffer.position() - mark < recommendedCount && buffer.position() != buffer.limit() )
            {
                isEnd = channel.read( buffer ) == -1;
            }
            buffer.limit( buffer.position() );
            buffer.position( mark );
            return isEnd ? EOF : ( buffer.remaining() >= recommendedCount ? OVERFLOW : UNDERFLOW );
        }
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

        public void reset()
        {
            i = 0;
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
                int length = Math.min( chunkSize, bytes.length - i );
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
        public void close() throws IOException
        {
        }
    }

    enum StreamReadStatus
    {
        UNDERFLOW,
        OVERFLOW,
        EOF
    }
}
