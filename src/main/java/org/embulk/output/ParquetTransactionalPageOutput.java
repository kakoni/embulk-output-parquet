// ParquetTransactionalPageOutput.java
package org.embulk.output;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.embulk.config.TaskReport;
import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.modules.ZoneIdModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

class ParquetTransactionalPageOutput implements TransactionalPageOutput {
    private static final Logger logger = LoggerFactory.getLogger(ParquetTransactionalPageOutput.class);
    
    private final PageReader reader;
    private final ParquetWriter<GenericRecord> writer;
    private final Schema avroSchema;
    private final org.embulk.spi.Schema embulkSchema;
    private final ParquetOutputPlugin.PluginTask task;
    private final int taskIndex;
    
    // Performance optimization
    private final int batchSize;
    private final List<GenericRecord> recordBatch;
    private final int progressLogInterval;
    
    // Metrics
    private final AtomicLong recordCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private long lastProgressLog = 0;
    private long startTime;
    
    // State management
    private volatile boolean closed = false;
    private volatile boolean failed = false;
    
    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder()
                .addDefaultModules()
                .addModule(ZoneIdModule.withLegacyNames())
                .build();
    
    public ParquetTransactionalPageOutput(PageReader reader,
                                         ParquetWriter<GenericRecord> writer,
                                         org.embulk.spi.Schema embulkSchema,
                                         Schema avroSchema,
                                         ParquetOutputPlugin.PluginTask task,
                                         int taskIndex) {
        this.reader = reader;
        this.writer = writer;
        this.embulkSchema = embulkSchema;
        this.avroSchema = avroSchema;
        this.task = task;
        this.taskIndex = taskIndex;
        
        this.batchSize = task.getBatchSize().orElse(1000);
        this.recordBatch = new ArrayList<>(batchSize);
        this.progressLogInterval = task.getProgressLogInterval().orElse(10000);
        this.startTime = System.currentTimeMillis();
        
        logger.debug("Initialized ParquetTransactionalPageOutput for task {} with batch size {}", 
                    taskIndex, batchSize);
    }
    
    @Override
    public void add(Page page) {
        if (closed) {
            throw new IllegalStateException("Output already closed for task " + taskIndex);
        }
        
        if (failed) {
            throw new IllegalStateException("Output has failed for task " + taskIndex);
        }
        
        reader.setPage(page);
        
        try {
            while (reader.nextRecord()) {
                GenericRecord record = convertToAvroRecord();
                recordBatch.add(record);
                
                // Write batch if full
                if (recordBatch.size() >= batchSize) {
                    flushBatch();
                }
                
                // Log progress
                logProgressIfNeeded();
            }
        } catch (Exception e) {
            failed = true;
            logger.error("Error processing page in task {}: {}", taskIndex, e.getMessage(), e);
            throw new RuntimeException("Failed to process page in task " + taskIndex, e);
        }
    }
    
    private GenericRecord convertToAvroRecord() {
        GenericRecord rec = new GenericData.Record(avroSchema);
        
        for (Column col : embulkSchema.getColumns()) {
            try {
                if (reader.isNull(col)) {
                    rec.put(col.getName(), null);
                    continue;
                }
                
                Object value = extractColumnValue(col);
                rec.put(col.getName(), value);
            } catch (Exception e) {
                errorCount.incrementAndGet();
                logger.warn("Error extracting value for column {} in task {}: {}", 
                           col.getName(), taskIndex, e.getMessage());
                rec.put(col.getName(), null); // Set null on error to avoid data loss
            }
        }
        
        return rec;
    }
    
    private Object extractColumnValue(Column col) {
        switch (col.getType().getName()) {
            case "boolean":
                return reader.getBoolean(col);
            case "long":
                return reader.getLong(col);
            case "double":
                return reader.getDouble(col);
            case "string":
                String str = reader.getString(col);
                // Handle potential large strings
                if (str != null && str.length() > 1_000_000) {
                    logger.warn("Large string value ({} chars) in column {} for task {}", 
                               str.length(), col.getName(), taskIndex);
                }
                return str;
            case "timestamp":
                Instant inst = reader.getTimestampInstant(col);
                return ParquetOutputPlugin.toEpochMicros(inst);
            case "json":
                throw new UnsupportedOperationException("JSON type is not supported for Parquet output");
            default:
                throw new UnsupportedOperationException("Unsupported type: " + col.getType().getName());
        }
    }
    
    private void flushBatch() throws IOException {
        if (recordBatch.isEmpty()) {
            return;
        }
        
        try {
            for (GenericRecord record : recordBatch) {
                writer.write(record);
                recordCount.incrementAndGet();
            }
            recordBatch.clear();
        } catch (IOException e) {
            logger.error("Failed to write batch of {} records in task {}", 
                        recordBatch.size(), taskIndex, e);
            throw e;
        }
    }
    
    private void logProgressIfNeeded() {
        long current = recordCount.get();
        if (current - lastProgressLog >= progressLogInterval) {
            long elapsed = System.currentTimeMillis() - startTime;
            double recordsPerSecond = (current * 1000.0) / elapsed;
            logger.info("Task {}: Written {} records ({:.2f} records/sec)", 
                       taskIndex, current, recordsPerSecond);
            lastProgressLog = current;
        }
    }
    
    @Override
    public void finish() {
        if (failed) {
            logger.warn("Finish called on failed output for task {}", taskIndex);
            return;
        }
        
        try {
            // Flush any remaining records
            flushBatch();
            
            // Log final statistics
            long totalRecords = recordCount.get();
            long totalErrors = errorCount.get();
            long elapsed = System.currentTimeMillis() - startTime;
            
            logger.info("Task {} finished: {} records written, {} errors, took {}ms", 
                       taskIndex, totalRecords, totalErrors, elapsed);
        } catch (IOException e) {
            failed = true;
            throw new RuntimeException("Failed to finish writing for task " + taskIndex, e);
        }
    }
    
    @Override
    public void close() {
        if (closed) {
            return; // Already closed
        }
        
        try {
            // Ensure any remaining data is flushed
            if (!failed) {
                flushBatch();
            }
        } catch (IOException e) {
            logger.error("Error flushing final batch in task {}", taskIndex, e);
        } finally {
            // Close writer
            try {
                writer.close();
                logger.debug("Closed Parquet writer for task {}", taskIndex);
            } catch (IOException e) {
                logger.error("Error closing Parquet writer for task {}", taskIndex, e);
            }
            
            // Close reader
            try {
                reader.close();
            } catch (Exception e) {
                logger.error("Error closing page reader for task {}", taskIndex, e);
            }
            
            closed = true;
        }
    }
    
    @Override
    public void abort() {
        logger.warn("Aborting Parquet output for task {} after {} records", 
                   taskIndex, recordCount.get());
        
        failed = true;
        
        // Best effort close
        try {
            writer.close();
        } catch (IOException e) {
            logger.debug("Error closing writer during abort for task {}", taskIndex, e);
        }
        
        try {
            reader.close();
        } catch (Exception e) {
            logger.debug("Error closing reader during abort for task {}", taskIndex, e);
        }
        
        closed = true;
    }
    
    @Override
    public TaskReport commit() {
        TaskReport report = CONFIG_MAPPER_FACTORY.newTaskReport();
        report.set("task_index", taskIndex);
        report.set("records", recordCount.get());
        report.set("errors", errorCount.get());
        report.set("closed", closed);
        report.set("failed", failed);
        
        long elapsed = System.currentTimeMillis() - startTime;
        report.set("elapsed_ms", elapsed);
        
        if (elapsed > 0) {
            double recordsPerSecond = (recordCount.get() * 1000.0) / elapsed;
            report.set("records_per_second", recordsPerSecond);
        }
        
        logger.debug("Task {} commit report: {} records, {} errors, {}ms elapsed", 
                    taskIndex, recordCount.get(), errorCount.get(), elapsed);
        
        return report;
    }
}
