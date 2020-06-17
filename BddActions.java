
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class BddActions {
	
	public static String host = AccountProperties.getHostDb();
	public static String database = AccountProperties.getDatabase();
	public static String password = AccountProperties.getPasswordDb();
	public static String user = AccountProperties.getUserDb();
	
	// methode d'insertion de data dans la base a partir du message de l'events hub
	public static String insertTemporaryPosition(Double x, Double y, String idbalise) throws SQLException, ClassNotFoundException {
		try
        {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e)
        {
            throw new ClassNotFoundException("PostgreSQL n'est pas detecte dans le path....", e);
        }

        System.out.println("PostgreSQL detecte dans le path.... OK.");

        Connection connexion = null;
    	String tableGeomPoint = "positionbalise"+ "_id" + idbalise;

        // Initialisation de l'objet de connexion
        try
        {
            String url = String.format("jdbc:postgresql://%s/%s", host, database);
            
            // setup des proprietes de connexion
            Properties properties = new Properties();
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            properties.setProperty("ssl", "true");

            // get connection
            connexion = DriverManager.getConnection(url, properties);
        }
        catch (SQLException e)
        {
            throw new SQLException("Fail de la Connexion a la database...", e);
        }
        if (connexion != null) 
        { 
            System.out.println("Connexion a la database OK...");
            try
            {
                // Drop table pour eviter d'avoir une erreur.
                Statement statement = connexion.createStatement();
                statement.execute("DROP TABLE IF EXISTS positionbalise" + "_id" + idbalise);
         

                statement.execute("CREATE TABLE positionbalise"+ "_id" + idbalise +  "(id serial PRIMARY KEY,  idbalise VARCHAR(50), geom geometry)");

                System.out.println("Table temporaire cree.");
                
    
                // Insert geometry et id de la balise
                int nRowsInserted = 0;
                PreparedStatement preparedStatement = connexion.prepareStatement("INSERT INTO positionbalise"+ "_id" + idbalise +  "(idbalise, geom) VALUES (?, ST_SetSRID(ST_MakePoint(?, ?), 4326));");
                System.out.println("Prepare Statement.");
                preparedStatement.setString(1, idbalise);
                preparedStatement.setDouble(2, x);
                preparedStatement.setDouble(3, y);
                nRowsInserted += preparedStatement.executeUpdate();

                System.out.println(String.format(" %d ligne(s) inseree(s) dans positionbalise"+ "_id" + idbalise , nRowsInserted));
    
    
            }
            catch (SQLException e)
            {
                throw new SQLException("Exception SQL...", e);
            }       
        }
        else {
            System.out.println("La connexion a la database a echouee.");
        }
        System.out.println("Execution PgSQL terminee.");
        return tableGeomPoint;
    }

	// methode de reprojection de notre message Events Hub
	public static void exportDataToBdd(String tableGeomPoint) throws SQLException, ClassNotFoundException {
		try
        {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e)
        {
            throw new ClassNotFoundException("PostgreSQL n'est pas detecte dans le path....", e);
        }

        System.out.println("PostgreSQL detecte dans le path.... OK.");

        Connection connexion = null;

        // Initialisation de l'objet de connexion
        try
        {
            String url = String.format("jdbc:postgresql://%s/%s", host, database);
            
            // setup des proprietes de connexion
            Properties properties = new Properties();
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            properties.setProperty("ssl", "true");

            // get connection
            connexion = DriverManager.getConnection(url, properties);
        }
        catch (SQLException e)
        {
            throw new SQLException("Fail de la Connexion a la database...", e);
        }
        if (connexion != null) 
        { 
            System.out.println("Connexion a la database OK...");
            try
            {
                Statement statement = connexion.createStatement();   

                statement.execute("DROP TABLE IF EXISTS reproj_" + tableGeomPoint + "; CREATE TABLE reproj_" + tableGeomPoint +  " as SELECT idbalise,\r\n" + 
                		"ST_ClosestPoint((SELECT  ST_Transform(geom, 2154) FROM \"rail\" WHERE \"rail\".idlineaire like "+ tableGeomPoint +".idbalise), (SELECT ST_Transform(geom, 2154) FROM " + tableGeomPoint + ")) \r\n" + 
                		"As geom\r\n" + 
                		"FROM " + tableGeomPoint);

                Statement statementSelect = connexion.createStatement();
                ResultSet  select = statementSelect.executeQuery("SELECT json_build_object(\r\n" + 
                		"'type', 'featureCollection',\r\n" + 
                		"\r\n" + 
                		"'features', json_agg(\r\n" + 	
                		"    json_build_object(\r\n" + 
                		"        'type',       'Feature',\r\n" + 
                		"        'id',         idbalise,\r\n" + 
                		"        'geometry',   ST_AsGeoJSON((st_transform(geom,4326)))::json,\r\n" + 
                		"        'properties', json_build_object('idbalise', idbalise)\r\n" + 
                		"    )\r\n" + 
                		")\r\n" + 
                		")\r\n" + 
                		"FROM reproj_" + tableGeomPoint + " ;");
                
                while (select.next()) {
                	System.out.println("Select : " + select.getString(1));
                	
                	@SuppressWarnings("deprecation")
					JsonObject jsonObject = new JsonParser().parse(select.getString(1)).getAsJsonObject();
                	WritingInBlob.write(jsonObject, tableGeomPoint.split("_")[1]);
                }
                System.out.println("Snapping de la position de la balise ID : " + tableGeomPoint.split("_")[1] + "... OK");
                statement.execute("DROP TABLE IF EXISTS " + tableGeomPoint);
                System.out.println("La table : " + tableGeomPoint + " a été supprimée.");

    
            }
            catch (SQLException e)
            {
                throw new SQLException("Exception SQL...", e);
            }       
        }
        else {
            System.out.println("La connexion a la database a echouee.");
        }
        System.out.println("Execution PgSQL terminee.");
    }
}
