package org.embulk.output;

import static org.junit.Assert.*;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.time.Instant;

import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.junit.Test;

/**
 * Minimal tests for basic functionality:
 * - Epoch micros conversion
 * - Avro schema mapping for basic types
 * - Writing a single Parquet file
 */
public class ParquetOutputPluginBasicTest {

  @Test
  public void testToEpochMicros_basicAndNull() {
    Instant instant = Instant.ofEpochSecond(1_700_000_000L, 123_456_789);
    long micros = ParquetOutputPlugin.toEpochMicros(instant);
    assertEquals(1_700_000_000_000_000L + 123_456L, micros);

    assertEquals(0L, ParquetOutputPlugin.toEpochMicros(null));
  }

  @Test
  public void testBuildAvroSchema_basicTypes() {
    Schema embulk = Schema.builder()
        .add("id", Types.LONG)
        .add("name", Types.STRING)
        .add("ts", Types.TIMESTAMP)
        .build();

    org.apache.avro.Schema avro = ParquetOutputPlugin.buildAvroSchema(embulk);
    assertEquals("embulk_record", avro.getName());
    assertEquals(3, avro.getFields().size());

    // Each field is a union of [null, TYPE]
    assertUnionWithType(avro.getField("id").schema(), org.apache.avro.Schema.Type.LONG);
    assertUnionWithType(avro.getField("name").schema(), org.apache.avro.Schema.Type.STRING);

    // Timestamp is LONG with logicalType millis or micros depending on environment
    org.apache.avro.Schema tsUnion = avro.getField("ts").schema();
    org.apache.avro.Schema tsSchema = nonNullFromUnion(tsUnion);
    assertEquals(org.apache.avro.Schema.Type.LONG, tsSchema.getType());
    assertNotNull(LogicalTypes.fromSchema(tsSchema));
  }

  @Test
  public void testWriteSingleParquetFile() throws Exception {
    ParquetOutputPlugin plugin = new ParquetOutputPlugin();

    File tempDir = Files.createTempDirectory("embulk-parquet-basic").toFile();
    String outputDir = tempDir.getAbsolutePath();

    // Minimal task proxy: only output_dir, prefix, and overwrite=true matter here
    ParquetOutputPlugin.PluginTask task = (ParquetOutputPlugin.PluginTask)
        java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] { ParquetOutputPlugin.PluginTask.class },
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "getOutputDir": return outputDir;
                case "getFilePrefix": return java.util.Optional.of("part");
                case "getOverwrite": return java.util.Optional.of(true);
                default:
                  if (java.util.Optional.class.isAssignableFrom(method.getReturnType())) {
                    return java.util.Optional.empty();
                  }
                  throw new UnsupportedOperationException("Unexpected method: " + method.getName());
              }
            });

    // Embulk and Avro schemas
    Schema embulkSchema = Schema.builder().add("id", Types.LONG).build();
    org.apache.avro.Schema avroSchema = ParquetOutputPlugin.buildAvroSchema(embulkSchema);

    // Private helpers via reflection (keeps test close to plugin logic)
    Method mCreatePath = ParquetOutputPlugin.class
        .getDeclaredMethod("createOutputPath", ParquetOutputPlugin.PluginTask.class, int.class);
    mCreatePath.setAccessible(true);
    Path outputPath = (Path) mCreatePath.invoke(plugin, task, 0);

    Method mCreateWriter = ParquetOutputPlugin.class
        .getDeclaredMethod(
            "createParquetWriter",
            ParquetOutputPlugin.PluginTask.class,
            org.apache.avro.Schema.class,
            Path.class);
    mCreateWriter.setAccessible(true);

    org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer =
        (org.apache.parquet.hadoop.ParquetWriter<GenericRecord>)
            mCreateWriter.invoke(plugin, task, avroSchema, outputPath);
    try {
      GenericRecord rec = new GenericData.Record(avroSchema);
      rec.put("id", 123L);
      writer.write(rec);
    } finally {
      writer.close();
    }

    File outFile = new File(outputPath.toUri());
    assertTrue("Parquet file should exist", outFile.exists());
    assertTrue("Parquet file should be non-empty", outFile.length() > 0L);
  }

  private static void assertUnionWithType(org.apache.avro.Schema union, org.apache.avro.Schema.Type expected) {
    org.apache.avro.Schema nonNull = nonNullFromUnion(union);
    assertEquals(expected, nonNull.getType());
  }

  private static org.apache.avro.Schema nonNullFromUnion(org.apache.avro.Schema union) {
    assertEquals(org.apache.avro.Schema.Type.UNION, union.getType());
    for (org.apache.avro.Schema s : union.getTypes()) {
      if (s.getType() != org.apache.avro.Schema.Type.NULL) {
        return s;
      }
    }
    throw new AssertionError("Union does not contain non-null type: " + union);
  }
}

