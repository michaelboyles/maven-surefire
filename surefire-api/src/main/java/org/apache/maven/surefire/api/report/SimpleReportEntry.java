package org.apache.maven.surefire.api.report;

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

import org.apache.maven.surefire.api.util.internal.ImmutableMap;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Basic implementation of {@link TestSetReportEntry} (immutable and thread-safe object).
 *
 * @author Kristian Rosenvold
 */
public class SimpleReportEntry
    implements TestSetReportEntry
{
    private final Map<String, String> systemProperties;

    private final String source;

    private final String sourceText;

    private final String name;

    private final String nameText;

    private final StackTraceWriter stackTraceWriter;

    private final Integer elapsed;

    private final String message;

    protected SimpleReportEntry( String source, String sourceText, String name, String nameText,
                                 StackTraceWriter stackTraceWriter, Integer elapsed, String message,
                                 Map<String, String> systemProperties )
    {
        this.source = source;
        this.sourceText = sourceText;
        this.name = name;
        this.nameText = nameText;
        this.stackTraceWriter = stackTraceWriter;
        this.message = message;
        this.elapsed = elapsed;
        this.systemProperties = new ImmutableMap<>( systemProperties );
    }

    /**
     * A builder for {@link SimpleReportEntry}s.
     */
    public static class Builder
    {
        private Map<String, String> systemProperties = Collections.emptyMap();
        private String source;
        private String sourceText;
        private String name;
        private String nameText;
        private StackTraceWriter stackTraceWriter;
        private Integer elapsed;
        private String message;

        public Builder systemProperties( Map<String, String> systemProperties )
        {
            this.systemProperties = systemProperties;
            return this;
        }

        public Builder source( String source, String sourceText )
        {
            this.source = source;
            this.sourceText = sourceText;
            return this;
        }

        public Builder name( String name, String nameText )
        {
            this.name = name;
            this.nameText = nameText;
            return this;
        }

        public Builder stackTraceWriter( StackTraceWriter stackTraceWriter )
        {
            this.stackTraceWriter = stackTraceWriter;
            return this;
        }

        public Builder stackTraceWriterAndMessage( StackTraceWriter stackTraceWriter )
        {
            this.stackTraceWriter = stackTraceWriter;
            this.message = safeGetMessage( stackTraceWriter );
            return this;
        }

        public Builder elapsed( Integer elapsed )
        {
            this.elapsed = elapsed;
            return this;
        }

        public Builder message( String message )
        {
            this.message = message;
            return this;
        }

        public SimpleReportEntry build()
        {
            return new SimpleReportEntry(
                source, sourceText, name, nameText, stackTraceWriter, elapsed, message, systemProperties
            );
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    protected static String safeGetMessage( StackTraceWriter stackTraceWriter )
    {
        try
        {
            SafeThrowable t = stackTraceWriter == null ? null : stackTraceWriter.getThrowable();
            return t == null ? null : t.getMessage();
        }
        catch ( Throwable t )
        {
            return t.getMessage();
        }
    }

    @Override
    public String getSourceName()
    {
        return source;
    }

    @Override
    public String getSourceText()
    {
        return sourceText;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getNameText()
    {
        return nameText;
    }

    @Override
    public String getGroup()
    {
        return null;
    }

    @Override
    public StackTraceWriter getStackTraceWriter()
    {
        return stackTraceWriter;
    }

    @Override
    public Integer getElapsed()
    {
        return elapsed;
    }

    @Override
    public int getElapsed( int fallback )
    {
        return elapsed == null ? fallback : elapsed;
    }

    @Override
    public String toString()
    {
        return "ReportEntry{" + "source='" + source + "', sourceText='" + sourceText
                + "', name='" + name + "', nameText='" + nameText + "', stackTraceWriter='"
                + stackTraceWriter + "', elapsed='" + elapsed + "', message='" + message + "'}";
    }

    @Override
    public String getMessage()
    {
        return message;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        SimpleReportEntry that = (SimpleReportEntry) o;
        return isSourceEqual( that ) && isSourceTextEqual( that )
                && isNameEqual( that ) && isNameTextEqual( that )
                && isStackEqual( that )
                && isElapsedTimeEqual( that )
                && isSystemPropertiesEqual( that )
                && isMessageEqual( that );
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hashCode( getSourceName() );
        result = 31 * result + Objects.hashCode( getSourceText() );
        result = 31 * result + Objects.hashCode( getName() );
        result = 31 * result + Objects.hashCode( getNameText() );
        result = 31 * result + Objects.hashCode( getStackTraceWriter() );
        result = 31 * result + Objects.hashCode( getElapsed() );
        result = 31 * result + Objects.hashCode( getSystemProperties() );
        result = 31 * result + Objects.hashCode( getMessage() );
        return result;
    }

    @Override
    public String getNameWithGroup()
    {
        return getSourceName();
    }

    @Override
    public String getReportNameWithGroup()
    {
        return getSourceText();
    }

    @Override
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    private boolean isElapsedTimeEqual( SimpleReportEntry en )
    {
        return Objects.equals( getElapsed(), en.getElapsed() );
    }

    private boolean isNameTextEqual( SimpleReportEntry en )
    {
        return Objects.equals( getNameText(), en.getNameText() );
    }

    private boolean isNameEqual( SimpleReportEntry en )
    {
        return Objects.equals( getName(), en.getName() );
    }

    private boolean isSourceEqual( SimpleReportEntry en )
    {
        return Objects.equals( getSourceName(), en.getSourceName() );
    }

    private boolean isSourceTextEqual( SimpleReportEntry en )
    {
        return Objects.equals( getSourceText(), en.getSourceText() );
    }

    private boolean isStackEqual( SimpleReportEntry en )
    {
        return Objects.equals( getStackTraceWriter(), en.getStackTraceWriter() );
    }

    private boolean isSystemPropertiesEqual( SimpleReportEntry en )
    {
        return Objects.equals( getSystemProperties(), en.getSystemProperties() );
    }

    private boolean isMessageEqual( SimpleReportEntry en )
    {
        return Objects.equals( getMessage(), en.getMessage() );
    }
}
