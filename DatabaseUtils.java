package cmt.cmtTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.microsoft.azure.eventhubs.EventHubException;

public class DatabaseUtils {

	private BackendJsonSender sender;

	public DatabaseUtils() {
		this.sender = new BackendJsonSender();
	}

	public void exportDataToBdd(JsonObject position)
			throws SQLException, ClassNotFoundException, EventHubException, IOException {
		System.out.println(" STARTING PGSQL PROCESS... ");
		Connection connexion = databaseConnection();

		if (connexion != null) {
			System.out.println("Connexion a la database OK...");
			//Query to reproject a point on a line
			try {
				Statement statementSelect = connexion.createStatement();
				ResultSet select = statementSelect.executeQuery("WITH geombaliseprojection as (SELECT '"
						+ position.get("idbalise") + "' as idbalise, "
						+ "ST_ClosestPoint((SELECT  ST_Transform(geom, 4326) FROM \"rail\" WHERE \"rail\".idlineaire like '69'), "
						+ "(SELECT ST_Transform(St_GeomFromText('POINT(" + position.get("x") + " " + position.get("y")
						+ ")', 4326), 4326))) as geom)" + "SELECT json_build_object('type', 'featureCollection',"
						+ "'features', json_agg(json_build_object("
						+ "        'type',       'Feature',        'id',         idbalise,"
						+ "        'geometry',   ST_AsGeoJSON((st_transform(geom,4326)))::json,"
						+ "        'properties', json_build_object('idbalise', idbalise))))"
						+ "FROM geombaliseprojection ;");

				while (select.next()) {
					@SuppressWarnings("deprecation")

					JsonObject trainPositionSnapped = new JsonParser().parse(select.getString(1)).getAsJsonObject();
					System.out.println("train Position Snapped : " + trainPositionSnapped);
					// Sending the result on 2 queues : to FrontEnd and to BlobStorage (appendBlob)
					sender.sendEventToEventHub(trainPositionSnapped, EventHubType.FRONT);
					sender.sendEventToEventHub(trainPositionSnapped, EventHubType.STORAGE);
					System.out.println("Send to the other queue...");
				}

			} catch (SQLException e) {
				throw new SQLException("Exception SQL...", e);
			}
		} else {
			System.out.println("La connexion a la database a echouee.");
		}
		System.out.println("Execution PgSQL terminee.");
	}

	public Connection databaseConnection() throws ClassNotFoundException, SQLException {
		System.out.println("Starting database connection...");
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new ClassNotFoundException("PostgreSQL n'est pas detecte dans le path....", e);
		}

		System.out.println("PostgreSQL detecte dans le path.... OK.");

		// Initialisation de l'objet de connexion
		try {
			String url = String.format("jdbc:postgresql://%s/%s", AccessUtils.host, AccessUtils.database);

			// setup des proprietes de connexion
			Properties properties = new Properties();
			properties.setProperty("user", AccessUtils.user);
			properties.setProperty("password", AccessUtils.password);
			properties.setProperty("ssl", "true");

			// get connection
			return DriverManager.getConnection(url, properties);
		} catch (SQLException e) {
			throw new SQLException("Fail de la Connexion a la database...", e);
		}
	}
}
