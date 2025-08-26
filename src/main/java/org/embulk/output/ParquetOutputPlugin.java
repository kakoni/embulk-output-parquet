// ParquetOutputPlugin.java
package org.embulk.output;

import io.github.shin1103.embulk.util.ClassLoaderSwap;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroParquetWriter.Builder;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Optional;

public class ParquetOutputPlugin implements OutputPlugin {
    private static final Logger logger = LoggerFactory.getLogger(ParquetOutputPlugin.class);
    
    // Configuration constants
    private static final String DEFAULT_FILE_PREFIX = "part";
    private static final String DEFAULT_COMPRESSION = "SNAPPY";
    private static final String FILE_PATTERN = "%s-%05d.parquet";
    private static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024; // 128MB
    private static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;   // 1MB
    private static final int DEFAULT_BATCH_SIZE = 1000;

    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder()
                .addDefaultModules()
                .addModule(ZoneIdModule.withLegacyNames())
                .build();
    protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    public interface PluginTask extends Task {
        @Config("output_dir")
        String getOutputDir();

        @Config("file_prefix")
        @ConfigDefault("\"part\"")
        Optional<String> getFilePrefix();

        @Config("compression")
        @ConfigDefault("\"SNAPPY\"")
        Optional<String> getCompression();
        
        @Config("overwrite")
        @ConfigDefault("true")
        Optional<Boolean> getOverwrite();
        
        @Config("row_group_size")
        @ConfigDefault("134217728") // 128MB
        Optional<Integer> getRowGroupSize();
        
        @Config("page_size")
        @ConfigDefault("1048576") // 1MB
        Optional<Integer> getPageSize();
        
        @Config("dictionary_encoding")
        @ConfigDefault("true")
        Optional<Boolean> getDictionaryEncoding();
        
        @Config("validation")
        @ConfigDefault("false")
        Optional<Boolean> getValidation();
        
        @Config("batch_size")
        @ConfigDefault("1000")
        Optional<Integer> getBatchSize();
        
        @Config("progress_log_interval")
        @ConfigDefault("10000")
        Optional<Integer> getProgressLogInterval();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, Control control) {
        try (ClassLoaderSwap<? extends ParquetOutputPlugin> ignored = new ClassLoaderSwap<>(this.getClass())) {
            final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
            validateAndPrepareTask(task);
            
            logger.info("Starting Parquet output with {} tasks, compression: {}", 
                       taskCount, task.getCompression().orElse(DEFAULT_COMPRESSION));
            
            control.run(task.toTaskSource());
            
            return CONFIG_MAPPER_FACTORY.newConfigDiff();
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException("Failed to configure Parquet output plugin", e);
        }
    }
    
    private void validateAndPrepareTask(PluginTask task) {
        // Validate output directory
        String outputDir = task.getOutputDir();
        if (outputDir == null || outputDir.trim().isEmpty()) {
            throw new ConfigException("output_dir must be specified");
        }
        
        // Create output directory if it doesn't exist
        File dir = new File(outputDir);
        try {
            if (!dir.exists()) {
                Files.createDirectories(dir.toPath());
                logger.info("Created output directory: {}", dir.getAbsolutePath());
            } else if (!dir.isDirectory()) {
                throw new ConfigException("output_dir exists but is not a directory: " + dir);
            }
        } catch (IOException e) {
            throw new ConfigException("Failed to create output directory: " + outputDir, e);
        }
        
        // Validate compression codec
        String compression = task.getCompression().orElse(DEFAULT_COMPRESSION);
        try {
            CompressionCodecName.fromConf(compression);
        } catch (Exception e) {
            throw new ConfigException("Invalid compression codec: " + compression, e);
        }
        
        // Validate numeric parameters
        if (task.getRowGroupSize().orElse(DEFAULT_BLOCK_SIZE) <= 0) {
            throw new ConfigException("row_group_size must be positive");
        }
        if (task.getPageSize().orElse(DEFAULT_PAGE_SIZE) <= 0) {
            throw new ConfigException("page_size must be positive");
        }
        if (task.getBatchSize().orElse(DEFAULT_BATCH_SIZE) <= 0) {
            throw new ConfigException("batch_size must be positive");
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, Control control) {
        throw new UnsupportedOperationException("embulk-output-parquet does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, 
                        java.util.List<TaskReport> successTaskReports) {
        // Log summary statistics
        long totalRecords = successTaskReports.stream()
            .mapToLong(report -> report.get(Long.class, "records", 0L))
            .sum();
        logger.info("Successfully wrote {} total records across {} tasks", totalRecords, taskCount);
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, org.embulk.spi.Schema embulkSchema, int taskIndex) {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
        
        try {
            // Build Avro schema from Embulk schema
            org.apache.avro.Schema avroSchema = buildAvroSchema(embulkSchema);
            
            // Create output path
            Path outputPath = createOutputPath(task, taskIndex);
            
            // Create Parquet writer
            ParquetWriter<GenericRecord> writer = createParquetWriter(task, avroSchema, outputPath);
            
            // Create page reader
            PageReader pageReader = Exec.getPageReader(embulkSchema);
            
            logger.info("Opened Parquet writer for task {} at: {}", taskIndex, outputPath);
            
            return new ParquetTransactionalPageOutput(
                pageReader, 
                writer, 
                embulkSchema, 
                avroSchema, 
                task,
                taskIndex
            );
        } catch (IOException e) {
            throw new ConfigException("Failed to create Parquet writer for task " + taskIndex, e);
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error opening Parquet output for task " + taskIndex, e);
        }
    }
    
    private Path createOutputPath(PluginTask task, int taskIndex) {
        String prefix = task.getFilePrefix().orElse(DEFAULT_FILE_PREFIX);
        String fileName = String.format(FILE_PATTERN, prefix, taskIndex);
        File outputFile = new File(task.getOutputDir(), fileName);
        
        logger.debug("Output file path for task {}: {}", taskIndex, outputFile.getAbsolutePath());
        return new Path(outputFile.toURI());
    }
    
    private ParquetWriter<GenericRecord> createParquetWriter(PluginTask task, 
                                                            org.apache.avro.Schema avroSchema, 
                                                            Path outputPath) throws IOException {
        Configuration hadoopConf = createHadoopConfiguration(task);
        CompressionCodecName codec = CompressionCodecName.fromConf(
            task.getCompression().orElse(DEFAULT_COMPRESSION)
        );
        ParquetFileWriter.Mode writeMode = task.getOverwrite().orElse(true) 
            ? ParquetFileWriter.Mode.OVERWRITE 
            : ParquetFileWriter.Mode.CREATE;
        
        Builder<GenericRecord> builder = AvroParquetWriter.<GenericRecord>builder(outputPath)
                .withSchema(avroSchema)
                .withConf(hadoopConf)
                .withWriteMode(writeMode)
                .withCompressionCodec(codec)
                .withRowGroupSize(task.getRowGroupSize().orElse(DEFAULT_BLOCK_SIZE))
                .withPageSize(task.getPageSize().orElse(DEFAULT_PAGE_SIZE))
                .withDictionaryEncoding(task.getDictionaryEncoding().orElse(true))
                .withValidation(task.getValidation().orElse(false));
        
        return builder.build();
    }
    
    private Configuration createHadoopConfiguration(PluginTask task) {
        Configuration conf = new Configuration(false);
        
        // Set performance-related configurations
        conf.setInt("parquet.block.size", task.getRowGroupSize().orElse(DEFAULT_BLOCK_SIZE));
        conf.setInt("parquet.page.size", task.getPageSize().orElse(DEFAULT_PAGE_SIZE));
        conf.setBoolean("parquet.compression", true);
        
        return conf;
    }

    static org.apache.avro.Schema buildAvroSchema(org.embulk.spi.Schema embulkSchema) {
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fields = 
            SchemaBuilder.record("embulk_record").fields();

        for (Column col : embulkSchema.getColumns()) {
            org.apache.avro.Schema fieldSchema = createFieldSchema(col);
            // All fields nullable to allow NULLs from Embulk
            fields = fields.name(col.getName())
                .type(org.apache.avro.Schema.createUnion(
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), 
                    fieldSchema))
                .withDefault(null);
        }

        return fields.endRecord();
    }
    
    private static org.apache.avro.Schema createFieldSchema(Column col) {
        switch (col.getType().getName()) {
            case "boolean":
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN);
            case "long":
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
            case "double":
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE);
            case "string":
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
            case "timestamp":
                org.apache.avro.Schema ts = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
                LogicalTypes.timestampMicros().addToSchema(ts);
                return ts;
            case "json":
                throw new UnsupportedOperationException("JSON type is not supported for Parquet output");
            default:
                throw new UnsupportedOperationException("Unsupported type: " + col.getType().getName());
        }
    }

    static long toEpochMicros(Instant instant) {
        if (instant == null) {
            return 0L;
        }
        long seconds = instant.getEpochSecond();
        int nano = instant.getNano();
        return seconds * 1_000_000L + (nano / 1_000);
    }
}
