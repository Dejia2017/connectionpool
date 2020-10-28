package cn.mengdejia.pool;

import java.sql.*;
import java.util.Enumeration;
import java.util.Vector;

/**
 * ①从连接池获取或创建可用连接；②使用完毕之后，把连接返还给连接池；③在系统关闭前，断开所有连接并释放连接占用的系统资源；
 * ④还能够处理无效连接（原来登记为可用的连接，由于某种原因不再可用，如超时，通讯问题），并能够限制连接池中的连接总数不低于某个预定值和不超过某个预定值。 @Author: Dejia
 * Meng @Date: 2020-10-28 14:31
 */
public class ConnectionPool {
  /** 数据库驱动 */
  private String jdbcDriver = "";
  /** 数据库URL */
  private String dbUrl = "";

  private String dbUsername = "";
  private String dbPassword = "";
  private String testTable = ""; // // 测试连接是否可用的测试表名，默认没有测试表
  private int initialConnections = 10;
  private int incrementalConnections = 5; // 连接池自动增加的大小
  private int maxConnections = 50;
  private Vector<PooledConnection> connections = null; // 存放连接池中数据库连接的向量 , 初始时为 null
  // 它中存放的对象为 PooledConnection 型

  public ConnectionPool(String jdbcDriver, String dbUrl, String dbUsername, String dbPassword) {
    this.jdbcDriver = jdbcDriver;
    this.dbUrl = dbUrl;
    this.dbUsername = dbUsername;
    this.dbPassword = dbPassword;
  }

  public int getInitialConnections() {
    return this.initialConnections;
  }

  /**
   * 设置连接池的初始大小
   *
   * @param //用于设置初始连接池中连接的数量
   */
  public void setInitialConnections(int initialConnections) {
    this.initialConnections = initialConnections;
  }

  /**
   * 返回连接池自动增加的大小 、
   *
   * @return 连接池自动增加的大小
   */
  public int getIncrementalConnections() {
    return this.incrementalConnections;
  }

  /**
   * 设置连接池自动增加的大小
   *
   * @param //连接池自动增加的大小
   */
  public void setIncrementalConnections(int incrementalConnections) {
    this.incrementalConnections = incrementalConnections;
  }

  /**
   * 返回连接池中最大的可用连接数量
   *
   * @return 连接池中最大的可用连接数量
   */
  public int getMaxConnections() {
    return this.maxConnections;
  }

  /**
   * 设置连接池中最大可用的连接数量
   *
   * @param // 设置连接池中最大可用的连接数量值
   */
  public void setMaxConnections(int maxConnections) {
    this.maxConnections = maxConnections;
  }

  /**
   * 获取测试数据库表的名字
   *
   * @return 测试数据库表的名字
   */
  public String getTestTable() {
    return this.testTable;
  }

  /**
   * 设置测试表的名字
   *
   * @param testTable String 测试表的名字
   */
  public void setTestTable(String testTable) {
    this.testTable = testTable;
  }

  public synchronized void createPool()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
    if (connections != null) {
      return;
    }
    // 实例化 JDBC Driver 中指定的驱动类实例
    Driver driver = (Driver) (Class.forName(this.jdbcDriver)).newInstance();
    // 注册 JDBC 驱动程序
    DriverManager.registerDriver(driver);
    // 创建保存连接的向量 , 初始时有 0 个元素
    connections = new Vector<PooledConnection>();
    // 根据 initialConnections 中设置的值，创建连接。
    createConnections(this.initialConnections);
    // System.out.println(" 数据库连接池创建成功！ ");
  }

  /**
   * 通用的 创建指定数量的连接
   *
   * @param numConnections
   */
  private void createConnections(int numConnections) {
    for (int i = 0; i < numConnections; ++i) {
      // 是否连接池中的数据库连接的数量己经达到最大？最大值由类成员 maxConnections
      // 指出，如果 maxConnections 为 0 或负数，表示连接数量没有限制。
      // 如果连接数己经达到最大，即退出。
      if (this.maxConnections > 0 && this.connections.size() >= maxConnections) {
        break;
      }
      try {
        connections.addElement(new PooledConnection(newConnection()));
      } catch (SQLException e) {
        System.out.println(" 创建数据库连接失败！ " + e.getMessage());
        e.printStackTrace();
      }
      // System.out.println(" 数据库连接己创建 ......");
    }
  }

  private Connection newConnection() throws SQLException {
    Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
    // 如果这是第一次创建数据库连接，即检查数据库，获得此数据库允许支持的
    // 最大客户连接数目

    // 整个判断的最终目的就是对最大连接数的限制 仅在首次创建时限制即可，driverMaxConnections应该是一个固定值。在这里判断的原因是只有获得
    // 连接才能知道数据库允许的最大连接数
    if (connections.isEmpty()) {
      DatabaseMetaData metaData = connection.getMetaData();
      // 数据库返回的 driverMaxConnections 若为 0 ，表示此数据库没有最大
      // 连接限制，或数据库的最大连接限制不知道
      // driverMaxConnections 为返回的一个整数，表示此数据库允许客户连接的数目
      // 如果连接池中设置的最大连接数量大于数据库允许的连接数目 , 则置连接池的最大
      // 连接数目为数据库允许的最大数目
      int driverMaxConnections = metaData.getMaxConnections();
      if (driverMaxConnections > 0 && this.maxConnections > driverMaxConnections) {
        // 其实是对最大连接数的修正
        this.maxConnections = driverMaxConnections;
      }
    }
    return connection;
  }

  public synchronized Connection getConnection() throws SQLException {
    if (connections == null) {
      return null;
    }
    Connection conn = getFreeConnection();
    while (conn == null) {
      wait(250);
      conn = getFreeConnection();
    }
    return conn;
  }

  private Connection getFreeConnection() throws SQLException {
    Connection connection = findFreeConnection();
    if (connection == null) {
      createConnections(incrementalConnections);
      connection = findFreeConnection();
      if (connection == null) {
        return null;
      }
    }
    return connection;
  }

  private Connection findFreeConnection() {
    Connection connection = null;
    PooledConnection pConn = null;
    Enumeration enumeration = connections.elements();
    while (enumeration.hasMoreElements()) {
      pConn = (PooledConnection) enumeration.nextElement();
      if (!pConn.isBusy()) {
        connection = pConn.getConnection();
        pConn.setBusy(true);

        if (!testConnection(connection)) {
          try {
            connection = newConnection();
          } catch (SQLException e) {
            System.out.println(" 创建数据库连接失败！ " + e.getMessage());
            return null;
          }
          pConn.setConnection(connection);
        }
        break; // 己经找到一个可用的连接，退出
      }
    }
    return connection;
  }

  private boolean testConnection(Connection conn) {
    try {
      if (testTable.equals("")) {
        // 如果测试表为空，试着使用此连接的 setAutoCommit() 方法
        // 来判断连接否可用（此方法只在部分数据库可用，如果不可用 ,
        // 抛出异常）。注意：使用测试表的方法更可靠
        conn.setAutoCommit(true);
      } else {
        Statement stmt = conn.createStatement();
        stmt.execute("select count(*) from " + testTable);
      }
    } catch (SQLException e) {
      closeConnection(conn);
      return false;
    }
    return true;
  }

  private void closeConnection(Connection connection) {
    try {
      connection.close();
    } catch (SQLException e) {
      System.out.println(" 关闭数据库连接出错： " + e.getMessage());
    }
  }

  /**
   * 此函数返回一个数据库连接到连接池中，并把此连接置为空闲。 所有使用连接池获得的数据库连接均应在不使用此连接时返回它。
   *
   * @param // 需返回到连接池中的连接对象
   */
  public void returnConnection(Connection conn) {
    if (this.connections == null) {
      return;
    }
    PooledConnection pooledConnection = null;
    Enumeration enumeration = connections.elements();
    while (enumeration.hasMoreElements()) {
      pooledConnection = (PooledConnection) enumeration.nextElement();
      if (conn == pooledConnection.getConnection()) {
        pooledConnection.setBusy(false);
        break;
      }
    }
  }

    /**
     * 关闭连接池中所有的连接，并清空连接池。
     */

    public synchronized void closeConnectionPool() throws SQLException {
        // 确保连接池存在，如果不存在，返回
        if (connections == null) {
            System.out.println(" 连接池不存在，无法关闭 !");
            return;
        }
        PooledConnection pConn = null;
        Enumeration enumerate = connections.elements();
        while (enumerate.hasMoreElements()) {
            pConn = (PooledConnection) enumerate.nextElement();
            // 如果忙，等 5 秒
            if (pConn.isBusy()) {
                wait(5000); // 等 5 秒
            }
            // 5 秒后直接关闭它
            closeConnection(pConn.getConnection());
            // 从连接池向量中删除它
            connections.removeElement(pConn);
        }
        // 置连接池为空
        connections = null;
    }

        /**
         * 刷新连接池中所有的连接对象
         *
         */

    public synchronized void refreshConnections() throws SQLException {
        // 确保连接池己创新存在
        if (connections == null) {
            System.out.println(" 连接池不存在，无法刷新 !");
            return;
        }
        PooledConnection pConn = null;
        Enumeration enumerate = connections.elements();
        while (enumerate.hasMoreElements()) {
            // 获得一个连接对象
            pConn = (PooledConnection) enumerate.nextElement();
            // 如果对象忙则等 5 秒 ,5 秒后直接刷新
            if (pConn.isBusy()) {
                wait(5000); // 等 5 秒
            }
            // 关闭此连接，用一个新的连接代替它。
            closeConnection(pConn.getConnection());
            pConn.setConnection(newConnection());
            pConn.setBusy(false);
        }
    }

        private void wait(int mSeconds) {
    try {
      Thread.sleep(mSeconds);
    } catch (InterruptedException e) {
    }
  }

  class PooledConnection {
    Connection connection = null; // 数据库连接
    boolean busy = false; // 此连接是否正在使用的标志，默认没有正在使用

    public PooledConnection(Connection connection) {
      this.connection = connection;
    }

    public Connection getConnection() {
      return this.connection;
    }

    public void setConnection(Connection connection) {
      this.connection = connection;
    }

    public boolean isBusy() {
      return this.busy;
    }

    public void setBusy(boolean busy) {
      this.busy = busy;
    }
  }
}
