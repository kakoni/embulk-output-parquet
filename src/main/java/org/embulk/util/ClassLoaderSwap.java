package org.embulk.util;

/**
 * Temporarily swaps the current thread's context ClassLoader to the plugin's ClassLoader
 * and restores it when closed.
 *
 * <p>This class is based on embulk-output-parquet ClassLoaderSwap.java
 * (see https://github.com/choplin/embulk-output-parquet/
 * blob/master/src/main/java/org/embulk/output/parquet/ClassLoaderSwap.java).</p>
 */
public class ClassLoaderSwap<T> implements AutoCloseable {
  private final ClassLoader orgClassLoader;
  private final Thread curThread;

  /**
   * Constructs a swapper for the given plugin class's ClassLoader.
   *
   * @param pluginClass the plugin class whose ClassLoader will be used
   */
  public ClassLoaderSwap(Class<T> pluginClass) {
    this.curThread = Thread.currentThread();
    ClassLoader pluginClassLoader = pluginClass.getClassLoader();
    this.orgClassLoader = curThread.getContextClassLoader();
    curThread.setContextClassLoader(pluginClassLoader);
  }

  /** Restores the original context ClassLoader. */
  @Override
  public void close() {
    curThread.setContextClassLoader(orgClassLoader);
  }
}
