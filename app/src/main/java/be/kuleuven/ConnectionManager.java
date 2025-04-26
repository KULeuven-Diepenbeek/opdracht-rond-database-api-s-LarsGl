package be.kuleuven;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public class ConnectionManager {
  private String connectionString;
  private Connection connection;

  public ConnectionManager(String connectionString, String user, String pwd) {
    try {
      this.connectionString = connectionString;
      this.connection = (Connection) DriverManager.getConnection(connectionString, user, pwd);
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      // System.out.println("Error connecting to database with connectionstring: " + connectionString + ", and user: "
      //     + user + ", and the given password.");
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public Connection getConnection() {
    return connection;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public void flushConnection() {
    try {
      connection.commit();
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void initTables() {
    try {
      URI path = Objects.requireNonNull(App.class.getClassLoader().getResource("initTableWithDummyData.sql"))
          .toURI();
      var sql = new String(Files.readAllBytes(Paths.get(path)));
      Statement statement = (Statement) connection.createStatement();
      statement.executeUpdate(sql);
      statement.close();
      connection.commit();
      System.out.println("Database table initialized with dummy data.");
    } catch (Exception e) {
      System.out.println("An Error occurred when trying to initialize database table");
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void verifyTableContentOfInit() {
    try {
      Statement statement = (Statement) connection.createStatement();
      var result = statement.executeQuery("SELECT COUNT(*) as cnt FROM speler;");
      while (result.next()) {
        assert result.getInt("cnt") == 8;
      }
      statement.close();
    } catch (AssertionError a) {
      System.out.println("The assertion of #rows == 8 failed");
      a.printStackTrace();
      throw new RuntimeException(a);
    } catch (Exception e) {
      System.out.println("Error when trying to verify initialized table");
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
