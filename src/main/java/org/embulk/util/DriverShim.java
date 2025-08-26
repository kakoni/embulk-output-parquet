package org.embulk.util;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A simple JDBC Driver shim that delegates to the provided Driver instance.
 *
 * <p>Useful when loading a JDBC driver via a specific ClassLoader to register with
 * DriverManager.</p>
 */
public class DriverShim implements Driver {
  private final Driver driver;

  /**
   * Creates a new Driver shim that delegates to the given Driver.
   *
   * @param d the underlying JDBC driver
   */
  public DriverShim(Driver d) {
    this.driver = d;
  }

  /** {@inheritDoc} */
  public Connection connect(String u, Properties p) throws SQLException {
    return this.driver.connect(u, p);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("AbbreviationAsWordInName")
  public boolean acceptsURL(String url) throws SQLException {
    return this.driver.acceptsURL(url);
  }

  /** {@inheritDoc} */
  public int getMajorVersion() {
    return this.driver.getMajorVersion();
  }

  /** {@inheritDoc} */
  public int getMinorVersion() {
    return this.driver.getMinorVersion();
  }

  /** {@inheritDoc} */
  public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
    return this.driver.getPropertyInfo(u, p);
  }

  /** {@inheritDoc} */
  public boolean jdbcCompliant() {
    return this.driver.jdbcCompliant();
  }

  /** {@inheritDoc} */
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return this.driver.getParentLogger();
  }
}
