import py4j.GatewayServer;
import io.redash.queryrunner.athena.AthenaJDBCConnector;
import com.amazonaws.athena.jdbc.AthenaDriver;

public class J4Py {
  public static void main(String[] args) {
    J4Py app = new J4Py();
    GatewayServer server = new GatewayServer(app);
    server.start();
  }
}
