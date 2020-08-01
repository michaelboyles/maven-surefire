package org.apache.maven.plugin.surefire.extensions;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.maven.plugin.surefire.booterclient.output.DeserializedStacktraceWriter;
import org.apache.maven.plugin.surefire.log.api.ConsoleLogger;
import org.apache.maven.surefire.api.booter.ForkedProcessEventType;
import org.apache.maven.surefire.api.event.ConsoleDebugEvent;
import org.apache.maven.surefire.api.event.ConsoleErrorEvent;
import org.apache.maven.surefire.api.event.ConsoleInfoEvent;
import org.apache.maven.surefire.api.event.ConsoleWarningEvent;
import org.apache.maven.surefire.api.event.ControlByeEvent;
import org.apache.maven.surefire.api.event.ControlNextTestEvent;
import org.apache.maven.surefire.api.event.ControlStopOnNextTestEvent;
import org.apache.maven.surefire.api.event.Event;
import org.apache.maven.surefire.api.event.JvmExitErrorEvent;
import org.apache.maven.surefire.api.event.StandardStreamErrEvent;
import org.apache.maven.surefire.api.event.StandardStreamErrWithNewLineEvent;
import org.apache.maven.surefire.api.event.StandardStreamOutEvent;
import org.apache.maven.surefire.api.event.StandardStreamOutWithNewLineEvent;
import org.apache.maven.surefire.api.event.SystemPropertyEvent;
import org.apache.maven.surefire.api.event.TestAssumptionFailureEvent;
import org.apache.maven.surefire.api.event.TestErrorEvent;
import org.apache.maven.surefire.api.event.TestFailedEvent;
import org.apache.maven.surefire.api.event.TestSkippedEvent;
import org.apache.maven.surefire.api.event.TestStartingEvent;
import org.apache.maven.surefire.api.event.TestSucceededEvent;
import org.apache.maven.surefire.api.event.TestsetCompletedEvent;
import org.apache.maven.surefire.api.event.TestsetStartingEvent;
import org.apache.maven.surefire.api.report.RunMode;
import org.apache.maven.surefire.api.report.StackTraceWriter;
import org.apache.maven.surefire.api.report.TestSetReportEntry;
import org.apache.maven.surefire.extensions.CloseableDaemonThread;
import org.apache.maven.surefire.extensions.EventHandler;
import org.apache.maven.surefire.extensions.ForkNodeArguments;
import org.apache.maven.surefire.extensions.util.CountdownCloseable;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;
import static java.nio.charset.CodingErrorAction.REPLACE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.maven.plugin.surefire.extensions.EventConsumerThread.StreamReadStatus.EOF;
import static org.apache.maven.plugin.surefire.extensions.EventConsumerThread.StreamReadStatus.OVERFLOW;
import static org.apache.maven.plugin.surefire.extensions.EventConsumerThread.StreamReadStatus.UNDERFLOW;
import static org.apache.maven.surefire.api.booter.Constants.MAGIC_NUMBER;
import static org.apache.maven.surefire.api.booter.Constants.STREAM_ENCODING;
import static org.apache.maven.surefire.api.report.CategorizedReportEntry.reportEntry;
import static org.apache.maven.surefire.api.report.RunMode.MODES;

/**
 *
 */
public class EventConsumerThread extends CloseableDaemonThread
{
    private static final String[] JVM_ERROR_PATTERNS =
        {
            "could not create the java virtual machine",
            "error occurred during initialization", // of VM, of boot layer
            "error:", // general errors
            "could not reserve enough space", "could not allocate", "unable to allocate", // memory errors
            "java.lang.module.findexception" // JPMS errors
        };
    private static final String PRINTABLE_JVM_NATIVE_STREAM = "Listening for transport dt_socket at address:";
    private static final byte[] MAGIC_NUMBER_BYTES = MAGIC_NUMBER.getBytes( US_ASCII );
    private static final int DELIMINATOR_LENGTH = 1;
    private static final int BYTE_LENGTH = 1;
    private static final int INT_LENGTH = 4;

    private final ReadableByteChannel channel;
    private final EventHandler<Event> eventHandler;
    private final CountdownCloseable countdownCloseable;
    private final ForkNodeArguments arguments;
    private volatile boolean disabled;

    public EventConsumerThread( @Nonnull String threadName,
                                @Nonnull ReadableByteChannel channel,
                                @Nonnull EventHandler<Event> eventHandler,
                                @Nonnull CountdownCloseable countdownCloseable,
                                @Nonnull ForkNodeArguments arguments )
    {
        super( threadName );
        this.channel = channel;
        this.eventHandler = eventHandler;
        this.countdownCloseable = countdownCloseable;
        this.arguments = arguments;
    }

    @Override
    public void run()
    {
        try ( ReadableByteChannel stream = channel;
              CountdownCloseable c = countdownCloseable; )
        {
            decode();
        }
        catch ( IOException e )
        {
            // not needed
        }
    }

    @Override
    public void disable()
    {
        disabled = true;
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @SuppressWarnings( "checkstyle:innerassignment" )
    private void decode() throws IOException
    {
        Map<Segment, ForkedProcessEventType> events = mapEventTypes();
        Memento memento = new Memento();
        BufferedStream line = new BufferedStream( 32 );
        memento.bb.limit( 0 );
        boolean streamContinues = true;

        start:
        do
        {
            if ( !streamContinues )
            {
                printExistingLine( line );
                return;
            }

            line.reset();
            memento.segment.reset();

            int readCount =
                DELIMINATOR_LENGTH + MAGIC_NUMBER_BYTES.length + DELIMINATOR_LENGTH + BYTE_LENGTH + DELIMINATOR_LENGTH;
            streamContinues = read( memento.bb, readCount ) != EOF;
            if ( memento.bb.remaining() < readCount )
            {
                //todo throw exception - broken stream
            }
            checkHeader( memento.bb, memento.segment );

            memento.eventType = events.get( readSegment( memento.bb ) );

            for ( SegmentType segmentType : nextSegmentType( memento ) )
            {
                if ( segmentType == null )
                {
                    break;
                }

                switch ( segmentType )
                {
                    case RUN_MODE:
                        memento.runMode = MODES.get( readSegment( memento ) );
                        break;
                    case STRING_ENCODING:
                        //todo handle exceptions
                        memento.charset = Charset.forName( readSegment( memento ) );
                        break;
                    case BYTES_INT_COUNTER:
                        memento.bytesCounter = readInt( memento.bb );
                        break;
                    case DATA_STRING:
                        memento.cb.clear();
                        int bytesCounter = memento.bytesCounter;
                        if ( bytesCounter == 0 )
                        {
                            memento.data.add( "" );
                        }
                        else if ( bytesCounter == 1 )
                        {
                            // handle the returned boolean
                            read( memento.bb, 1 );
                            byte oneChar = memento.bb.get();
                            memento.data.add( oneChar == 0 ? null : String.valueOf( (char) oneChar ) );
                        }
                        else
                        {
                            memento.data.add( readString( memento ) );
                        }
                        memento.bytesCounter = 0;
                        break;
                    case DATA_INT:
                        memento.data.add( readInt( memento.bb ) );
                        break;
                    case END_OF_FRAME:
                        eventHandler.handleEvent( toEvent( memento ) );
                        continue start;
                    default:
                        throw new IllegalArgumentException( "Unknown enum " + segmentType );
                }

                memento.cb.clear();

                read( memento.bb, 1 );
                b = 0xff & memento.bb.get();
                if ( Character.isDefined( b ) && b != ':' )
                {
                    //memento.segmentCompletion = SegmentCompletion.;
                    //MalformedStreamException
                    continue start;
                }
            }

            memento.bb.flip();
            line.write( memento.bb );
            memento.reset();
        }
        while ( true );
    }

    @Nonnull
    private static Segment readSegment( ByteBuffer bb )
    {
        int readCount = bb.get() & 0xff;
        Segment segment = new Segment( bb.array(), bb.arrayOffset() + bb.position(), readCount );
        bb.position( bb.position() + readCount );
        checkDelimiter( bb );
        return segment;
    }

    private static void checkHeader( ByteBuffer bb, BufferedStream segment )
    {
        checkDelimiter( bb );

        segment.reset();

        int shift = 0;
        for ( int start = bb.arrayOffset() + bb.position(), length = MAGIC_NUMBER_BYTES.length;
              shift < length; shift++ )
        {
            if ( bb.array()[shift + start] != MAGIC_NUMBER_BYTES[shift] )
            {
                //todo throw exception - broken stream
            }
        }
        bb.position( bb.position() + shift );

        checkDelimiter( bb );
    }

    private static void checkDelimiter( ByteBuffer bb )
    {
        if ( ( 0xff & bb.get() ) != ':' )
        {
            //todo throw exception - broken stream
        }
    }

    private static SegmentType[] nextSegmentType( Memento memento )
    {
        switch ( memento.eventType )
        {
            case BOOTERCODE_BYE:
            case BOOTERCODE_STOP_ON_NEXT_TEST:
            case BOOTERCODE_NEXT_TEST:
                return new SegmentType[] {SegmentType.END_OF_FRAME};
            case BOOTERCODE_CONSOLE_ERROR:
            case BOOTERCODE_JVM_EXIT_ERROR:
                return new SegmentType[] {
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.STRING_ENCODING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.END_OF_FRAME
                };
            case BOOTERCODE_CONSOLE_INFO:
            case BOOTERCODE_CONSOLE_DEBUG:
            case BOOTERCODE_CONSOLE_WARNING:
                return new SegmentType[] {
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.STRING_ENCODING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.END_OF_FRAME
                };
            case BOOTERCODE_STDOUT:
            case BOOTERCODE_STDOUT_NEW_LINE:
            case BOOTERCODE_STDERR:
            case BOOTERCODE_STDERR_NEW_LINE:
                return new SegmentType[] {
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.RUN_MODE,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.STRING_ENCODING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.END_OF_FRAME
                };
            case BOOTERCODE_SYSPROPS:
                return new SegmentType[] {
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.RUN_MODE,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.STRING_ENCODING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.END_OF_FRAME
                };
            case BOOTERCODE_TESTSET_STARTING:
            case BOOTERCODE_TESTSET_COMPLETED:
            case BOOTERCODE_TEST_STARTING:
            case BOOTERCODE_TEST_SUCCEEDED:
            case BOOTERCODE_TEST_FAILED:
            case BOOTERCODE_TEST_SKIPPED:
            case BOOTERCODE_TEST_ERROR:
            case BOOTERCODE_TEST_ASSUMPTIONFAILURE:
                return new SegmentType[] {
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.RUN_MODE,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.STRING_ENCODING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.DATA_INT,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.BYTES_INT_COUNTER,
                    SegmentType.DATA_STRING,
                    SegmentType.END_OF_FRAME
                };
            default:
                throw new IllegalArgumentException( "Unknown enum " + memento.eventType );
        }
    }

    private StreamReadStatus read( ByteBuffer buffer, int recommendedCount ) throws IOException
    {
        if ( buffer.remaining() >= recommendedCount && buffer.position() != 0 )
        {
            return OVERFLOW;
        }
        else
        {
            if ( buffer.position() != 0 && recommendedCount > buffer.capacity() - buffer.position() )
            {
                buffer.compact().flip();
            }
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

    private void printExistingLine( BufferedStream line )
    {
        if ( line.isEmpty() )
        {
            return;
        }
        ConsoleLogger logger = arguments.getConsoleLogger();
        String s = line.toString( STREAM_ENCODING ).trim();
        if ( s.contains( PRINTABLE_JVM_NATIVE_STREAM ) )
        {
            if ( logger.isDebugEnabled() )
            {
                logger.debug( s );
            }
            else if ( logger.isInfoEnabled() )
            {
                logger.info( s );
            }
            else
            {
                // In case of debugging forked JVM, see PRINTABLE_JVM_NATIVE_STREAM.
                System.out.println( s );
            }
        }
        else
        {
            if ( isJvmError( s ) )
            {
                logger.error( s );
            }
            String msg = "Corrupted STDOUT by directly writing to native stream in forked JVM "
                + arguments.getForkChannelId() + ".";
            File dumpFile = arguments.dumpStreamText( msg + " Stream '" + s + "'." );
            arguments.logWarningAtEnd( msg + " See FAQ web page and the dump file " + dumpFile.getAbsolutePath() );

            if ( logger.isDebugEnabled() )
            {
                logger.debug( s );
            }
        }
    }

    private Event toEvent( Memento memento )
    {
        ForkedProcessEventType event = memento.eventType;
        if ( event.isControlCategory() )
        {
            switch ( event )
            {
                case BOOTERCODE_BYE:
                    return new ControlByeEvent();
                case BOOTERCODE_STOP_ON_NEXT_TEST:
                    return new ControlStopOnNextTestEvent();
                case BOOTERCODE_NEXT_TEST:
                    return new ControlNextTestEvent();
                default:
                    throw new IllegalStateException( "Unknown enum " + event );
            }
        }
        else if ( event.isConsoleErrorCategory() || event.isJvmExitError() )
        {
            String traceMessage = (String) memento.data.get( 0 );
            String smartTrimmedStackTrace = (String) memento.data.get( 1 );
            String stackTrace = (String) memento.data.get( 2 );
            StackTraceWriter stackTraceWriter = newTrace( traceMessage, smartTrimmedStackTrace, stackTrace );
            return event.isConsoleErrorCategory()
                ? new ConsoleErrorEvent( stackTraceWriter )
                : new JvmExitErrorEvent( stackTraceWriter );
        }
        else if ( event.isConsoleCategory() )
        {
            String msg = (String) memento.data.get( 0 );
            switch ( event )
            {
                case BOOTERCODE_CONSOLE_INFO:
                    return new ConsoleInfoEvent( msg );
                case BOOTERCODE_CONSOLE_DEBUG:
                    return new ConsoleDebugEvent( msg );
                case BOOTERCODE_CONSOLE_WARNING:
                    return new ConsoleWarningEvent( msg );
                default:
                    throw new IllegalStateException( "Unknown enum " + event );
            }
        }
        else if ( event.isStandardStreamCategory() )
        {
            String output = (String) memento.data.get( 0 );
            switch ( event )
            {
                case BOOTERCODE_STDOUT:
                    return new StandardStreamOutEvent( memento.runMode, output );
                case BOOTERCODE_STDOUT_NEW_LINE:
                    return new StandardStreamOutWithNewLineEvent( memento.runMode, output );
                case BOOTERCODE_STDERR:
                    return new StandardStreamErrEvent( memento.runMode, output );
                case BOOTERCODE_STDERR_NEW_LINE:
                    return new StandardStreamErrWithNewLineEvent( memento.runMode, output );
                default:
                    throw new IllegalStateException( "Unknown enum " + event );
            }
        }
        else if ( event.isSysPropCategory() )
        {
            String key = (String) memento.data.get( 0 );
            String value = (String) memento.data.get( 1 );
            return new SystemPropertyEvent( memento.runMode, key, value );
        }
        else if ( event.isTestCategory() )
        {
            // ReportEntry:
            String source = (String) memento.data.get( 0 );
            String sourceText = (String) memento.data.get( 1 );
            String name = (String) memento.data.get( 2 );
            String nameText = (String) memento.data.get( 3 );
            String group = (String) memento.data.get( 4 );
            String message = (String) memento.data.get( 5 );
            Integer timeElapsed = (Integer) memento.data.get( 6 );
            // StackTraceWriter:
            String traceMessage = (String) memento.data.get( 7 );
            String smartTrimmedStackTrace = (String) memento.data.get( 8 );
            String stackTrace = (String) memento.data.get( 9 );
            TestSetReportEntry reportEntry = newReportEntry( source, sourceText, name, nameText, group, message,
                timeElapsed, traceMessage, smartTrimmedStackTrace, stackTrace );

            switch ( event )
            {
                case BOOTERCODE_TESTSET_STARTING:
                    return new TestsetStartingEvent( memento.runMode, reportEntry );
                case BOOTERCODE_TESTSET_COMPLETED:
                    return new TestsetCompletedEvent( memento.runMode, reportEntry );
                case BOOTERCODE_TEST_STARTING:
                    return new TestStartingEvent( memento.runMode, reportEntry );
                case BOOTERCODE_TEST_SUCCEEDED:
                    return new TestSucceededEvent( memento.runMode, reportEntry );
                case BOOTERCODE_TEST_FAILED:
                    return new TestFailedEvent( memento.runMode, reportEntry );
                case BOOTERCODE_TEST_SKIPPED:
                    return new TestSkippedEvent( memento.runMode, reportEntry );
                case BOOTERCODE_TEST_ERROR:
                    return new TestErrorEvent( memento.runMode, reportEntry );
                case BOOTERCODE_TEST_ASSUMPTIONFAILURE:
                    return new TestAssumptionFailureEvent( memento.runMode, reportEntry );
                default:
                    throw new IllegalStateException( "Unknown enum " + event );
            }
        }

        throw new IllegalStateException( "Missing a branch for the event type " + event );
    }

    private static StackTraceWriter newTrace( String traceMessage, String smartTrimmedStackTrace, String stackTrace )
    {
        boolean exists = traceMessage != null || stackTrace != null || smartTrimmedStackTrace != null;
        return exists ? new DeserializedStacktraceWriter( traceMessage, smartTrimmedStackTrace, stackTrace ) : null;
    }

    static TestSetReportEntry newReportEntry( // ReportEntry:
                                              String source, String sourceText, String name,
                                              String nameText, String group, String message,
                                              Integer timeElapsed,
                                              // StackTraceWriter:
                                              String traceMessage,
                                              String smartTrimmedStackTrace, String stackTrace )
        throws NumberFormatException
    {
        StackTraceWriter stackTraceWriter = newTrace( traceMessage, smartTrimmedStackTrace, stackTrace );
        return reportEntry( source, sourceText, name, nameText, group, stackTraceWriter, timeElapsed, message,
            Collections.<String, String>emptyMap() );
    }

    private static boolean isJvmError( String line )
    {
        String lineLower = line.toLowerCase();
        for ( String errorPattern : JVM_ERROR_PATTERNS )
        {
            if ( lineLower.contains( errorPattern ) )
            {
                return true;
            }
        }
        return false;
    }

    private String readSegment( Memento memento ) throws IOException
    {
        int startsWithPosition = memento.bb.position();
        boolean streamContinues;

        do
        {
            streamContinues = read( memento.bb, 1 ) != EOF;
        }
        while ( ( 0xff & memento.bb.get() ) != ':' );

        int endsWithLimit = memento.bb.limit();
        memento.bb.limit( memento.bb.position() - 1 );
        memento.bb.position( startsWithPosition );

        memento.cb.clear();

        memento.decoder
            .reset()
            .decode( memento.bb, memento.cb, true );

        memento.bb.position( memento.bb.position() + 1 );
        memento.bb.limit( endsWithLimit );

        String segment = memento.cb.flip().toString();
        memento.cb.clear();
        return segment;
    }

    String readString( Memento memento ) throws IOException
    {
        final CharBuffer chars = memento.cb;
        final ByteBuffer buffer = memento.bb;
        final int totalBytes = memento.bytesCounter;

        int countDecodedBytes = 0;
        int countReadBytes = 0;

        buffer.clear();

        int positionChars = chars.position();
        int startPosition;
        List<String> strings = new ArrayList<>();
        do
        {
            startPosition = buffer.position();
            buffer.limit( startPosition );
            read( buffer, totalBytes - countReadBytes );
            memento.decoder
                .reset()
                .decode( buffer, chars, countDecodedBytes + buffer.remaining() >= totalBytes );
            final boolean hasDecodedNewBytes = chars.position() != positionChars;
            if ( hasDecodedNewBytes )
            {
                countDecodedBytes += buffer.position() - startPosition;
                positionChars = chars.position();
            }
            countReadBytes += buffer.limit() - startPosition;
            if ( buffer.hasRemaining() )
            {
                buffer.compact();
            }
            strings.add( chars.flip().toString() );
            chars.clear();
        }
        while ( countReadBytes < totalBytes );

        return toString( strings );
    }

    int readInt( ByteBuffer bb ) throws IOException
    {
        read( bb, 4 );
        return bb.getInt();
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

    static Map<Segment, ForkedProcessEventType> mapEventTypes()
    {
        Map<Segment, ForkedProcessEventType> map = new HashMap<>();
        for (ForkedProcessEventType e : ForkedProcessEventType.values() )
        {
            byte[] array = e.getOpcode().getBytes();
            map.put( new Segment( array, 0, array.length ), e );
        }
        return map;
    }

    enum StreamReadStatus
    {
        UNDERFLOW,
        OVERFLOW,
        EOF
    }

    /**
     * Determines whether the frame is complete or malformed.
     */
    private enum FrameCompletion
    {
        NOT_COMPLETE,
        COMPLETE,
        MALFORMED
    }

    private enum SegmentType
    {
        RUN_MODE,
        STRING_ENCODING,
        BYTES_INT_COUNTER,
        DATA_STRING,
        DATA_INT,
        END_OF_FRAME
    }

    /**
     * This class avoids locking which gains the performance of this decoder.
     */
    private static class BufferedStream
    {
        private byte[] buffer;
        private int count;

        public BufferedStream( int capacity )
        {
            this.buffer = new byte[capacity];
        }

        void write( int b )
        {
            ensureCapacity( 1 );
            buffer[count++] = (byte) b;
        }

        void write( ByteBuffer bb )
        {
            int size = bb.remaining();
            if ( size > 0 )
            {
                ensureCapacity( size );
                byte[] b = bb.array();
                int pos = bb.arrayOffset() + bb.position();
                System.arraycopy( b, pos, buffer, count, size );
                count+= size;
                bb.position( bb.position() + size );
            }
        }

        boolean isEmpty()
        {
            return count != 0;
        }

        void reset()
        {
            count = 0;
        }

        String toString( Charset charset )
        {
            return new String( buffer, 0, count, charset );
        }

        private void ensureCapacity( int addCapacity )
        {
            int oldCapacity = buffer.length;
            int exactCapacity = count + addCapacity;
            if ( exactCapacity < 0 )
            {
                throw new OutOfMemoryError();
            }

            if ( oldCapacity < exactCapacity )
            {
                int newCapacity = oldCapacity << 1;
                buffer = Arrays.copyOf( buffer, max( newCapacity, exactCapacity ) );
            }
        }
    }

    static class Memento
    {
        final CharsetDecoder decoder;
        final BufferedStream segment = new BufferedStream( MAGIC_NUMBER.length() );
        final List<Object> data = new ArrayList<>();
        final CharBuffer cb = CharBuffer.allocate( 1024 );
        final ByteBuffer bb = ByteBuffer.allocate( 1024 );
        FrameCompletion frameCompletion;
        ForkedProcessEventType eventType;
        RunMode runMode;
        Charset charset;
        int bytesCounter;

        Memento()
        {
            decoder = STREAM_ENCODING.newDecoder()
                .onMalformedInput( REPLACE )
                .onUnmappableCharacter( REPLACE );
        }

        void reset()
        {
            segment.reset();
            frameCompletion = null;
        }
    }

    static class Segment
    {
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
}
