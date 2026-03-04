package com.example.flink.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Zero-GC BulkFormat that reads text files or GZIP files
 * with a custom delimiter and attaches filename to each line.
 */
public class TextLineWithFileNameBulkFormat implements BulkFormat<Tuple2<String, String>, FileSourceSplit> {

    private static final int BUFFER_SIZE = 128 * 1024;
    private final byte[] delimiter;

    /** Constructor with custom delimiter */
    public TextLineWithFileNameBulkFormat(byte[] delimiter) {
        this.delimiter = delimiter;
    }

    /** Default constructor (\n) */
    public TextLineWithFileNameBulkFormat() {
        this(new byte[]{'\n'});
    }

    @Override
    public Reader<Tuple2<String, String>> createReader(Configuration config, FileSourceSplit split) throws IOException {
        return createReader(split, 0L);
    }

    @Override
    public Reader<Tuple2<String, String>> restoreReader(Configuration config, FileSourceSplit split) throws IOException {
        long offset = split.getReaderPosition()
                .map(CheckpointedPosition::getOffset)
                .orElse(split.start());
        return createReader(split, offset);
    }

    private Reader<Tuple2<String, String>> createReader(FileSourceSplit split, long startOffset) throws IOException {
        Path path = split.path();
        String fileName = path.getName();
        FSDataInputStream fsStream = path.getFileSystem().open(path);
        boolean isGzip = fileName.endsWith(".gz");

        if (!isGzip) fsStream.seek(startOffset);
        InputStream input = isGzip ? new GZIPInputStream(fsStream) : fsStream;

        long splitStart = split.start();
        long splitEnd = split.start() + split.length();

        LineReader reader = new LineReader(input, isGzip ? 0 : startOffset,
                isGzip ? Long.MAX_VALUE : splitEnd,
                !isGzip && startOffset > splitStart, delimiter);

        return new Reader<>() {
            @Override
            public RecordIterator<Tuple2<String, String>> readBatch() throws IOException {
                return new RecordIterator<>() {
                    Tuple2<String, String> nextRecord = readNext();

                    private Tuple2<String, String> readNext() throws IOException {
                        String line = reader.readLine();
                        if (line == null) return null;
                        return Tuple2.of(fileName, line);
                    }

                    @Override
                    public Tuple2<String, String> next() {
                        Tuple2<String, String> current = nextRecord;
                        if (current != null) {
                            try {
                                nextRecord = readNext();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return current;
                    }

                    @Override
                    public void releaseBatch() {}
                };
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }

            @Override
            public CheckpointedPosition getCheckpointedPosition() {
                return new CheckpointedPosition(reader.getCurrentOffset(), 0);
            }
        };
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    // ------------------------------------------------------------------------
    static class LineReader {
        private final InputStream input;
        private final long splitEnd;
        private final byte[] buffer = new byte[BUFFER_SIZE];
        private final byte[] delimiter;
        private int bufferPos = 0, bufferLimit = 0;
        private long currentOffset;
        private boolean skipFirstPartial;
        private int matchPos = 0;

        LineReader(InputStream input, long startOffset, long splitEnd,
                   boolean skipFirstPartial, byte[] delimiter) throws IOException {
            this.input = input;
            this.currentOffset = startOffset;
            this.splitEnd = splitEnd;
            this.skipFirstPartial = skipFirstPartial;
            this.delimiter = delimiter;

            if (skipFirstPartial) discardPartialLine();
        }

        private void discardPartialLine() throws IOException {
            while (true) {
                if (!fillBuffer()) break;
                while (bufferPos < bufferLimit) {
                    if (buffer[bufferPos++] == delimiter[0]) {
                        skipFirstPartial = false;
                        matchPos = 0;
                        return;
                    }
                }
            }
        }

        String readLine() throws IOException {
            if (currentOffset >= splitEnd) return null;

            int lineStart = bufferPos;
            while (true) {
                if (!fillBuffer()) break;

                for (; bufferPos < bufferLimit; bufferPos++) {
                    if (buffer[bufferPos] == delimiter[matchPos]) {
                        matchPos++;
                        if (matchPos == delimiter.length) {
                            int lineEnd = bufferPos - delimiter.length + 1;
                            String line = new String(buffer, lineStart, lineEnd - lineStart, StandardCharsets.UTF_8);
                            bufferPos++;
                            currentOffset += (lineEnd - lineStart) + delimiter.length;
                            matchPos = 0;
                            return line;
                        }
                    } else {
                        matchPos = 0;
                    }
                }
            }

            // last line (no delimiter at end)
            if (bufferPos - lineStart > 0) {
                String line = new String(buffer, lineStart, bufferPos - lineStart, StandardCharsets.UTF_8);
                currentOffset += bufferPos - lineStart;
                return line;
            }

            return null;
        }

        private boolean fillBuffer() throws IOException {
            if (bufferPos < bufferLimit) return true;
            if (currentOffset >= splitEnd) return false;
            bufferLimit = input.read(buffer);
            bufferPos = 0;
            return bufferLimit > 0;
        }

        long getCurrentOffset() { return currentOffset; }

        void close() throws IOException { input.close(); }
    }
}
