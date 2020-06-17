
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;



public class SenderEventsHub  
{
	   static String consumerGroupName = AccountProperties.getConsumerGroupName();
	   static String namespaceName = AccountProperties.getNamespaceName();
	   static String eventHubName = AccountProperties.getEventHubName();
	   static String sasKeyName = AccountProperties.getSasKeyName();
	   static String sasKey = AccountProperties.getSasKey();
	   final static String storageConnectionString = AccountProperties.getStorageConnectionString();
	   static String storageContainerName = AccountProperties.getStorageContainerName();
	   static String hostNamePrefix = AccountProperties.getHostNamePrefix();
	   static String accountName =  AccountProperties.getAccountName();

	   
   	final static ConnectionStringBuilder connStr = new ConnectionStringBuilder()
            .setNamespaceName(namespaceName) 
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey);
   	
   	static EventProcessorHost host = new EventProcessorHost(
	 			EventProcessorHost.createHostName(hostNamePrefix),
	 			eventHubName,
	 			consumerGroupName,
	 			connStr.toString(),
	 			storageConnectionString,
	 			storageContainerName);
   		   
    public static void main( String[] args ) throws Exception
    {
    	 // Creation d'un xy random avec un id a envoyer a l'events Hub en format Json
    	
    	JSONObject positionBalise = new JSONObject();
    	positionBalise.put("X",  -0.00);
    	positionBalise.put("Y",  5.88);
    	positionBalise.put("IDBALISE", rndChar());
    	writeMessage(positionBalise);

    }
    
    public static void writeMessage( JSONObject pos ) throws EventHubException, IOException {
    	final Gson gson = new GsonBuilder().create();
    	
    	System.out.println("Position initiale du train : " + pos.get("X") + ", " + pos.get("Y"));

        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);


        final EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);
        System.out.println( "Client : " + ehClient );

        try {
                            	
                byte[] payloadBytes = gson.toJson( pos ).getBytes( Charset.defaultCharset() );
                EventData sendEvent = EventData.create( payloadBytes );
                System.out.println( "Return of payload : " + pos );
                
                ehClient.sendSync( sendEvent );

            System.out.println( Instant.now() + ": Send Complete...");

        } finally {
            ehClient.closeSync();
            executorService.shutdown();
        }	
    	
        System.out.println( connStr );
        System.out.println("End of Receiver");
    }

    public static char rndChar() {
    	int rnd = (int) (Math.random() * 52);
    	char base = (rnd < 26) ? 'A' : 'a';
    	return (char) (base + rnd % 26);
    }
        
}
