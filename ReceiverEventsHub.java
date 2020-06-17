
import java.util.concurrent.ExecutionException;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;

// Petit module d'ecoute d'un Events Hub Azure

public class ReceiverEventsHub
{
	   static String eventHubName = "MonEventHubName";
	   static String sasKeyName = "MaSasKeyName";
	   static String sasKey = "MaSasKey";
	   final static String storageConnectionString = "MaStorageConnectionKey";
	   static String storageContainerName = "MonStorageContainerName";
	   static String hostNamePrefix = "MonHostNamePrefix";
	   static String accountName =  "MonAccountName";
	  
	   static final ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder()
			.setNamespaceName(AccountProperties.getNamespaceName())
			.setEventHubName(eventHubName)
			.setSasKeyName(sasKeyName)
			.setSasKey(sasKey);

	   static EventProcessorHost host = new EventProcessorHost(
			EventProcessorHost.createHostName(hostNamePrefix),
			eventHubName,
			AccountProperties.getConsumerGroupName(),
			eventHubConnectionString.toString(),
			storageConnectionString,
			storageContainerName);
	   
    public static void main(String args[]) throws InterruptedException, ExecutionException
    {

    	getEvents();
    	
    }
    
    public static void getEvents() throws InterruptedException, ExecutionException {

  	   System.out.println("Connexion a l'host : " + host.getHostName());
  	   EventProcessorOptions options = new EventProcessorOptions();

  	   host.registerEventProcessor(EventProcessor.class, options)
  	   .whenComplete((unused, e) ->
  	   {
  		   if (e != null)
  		   {
  			   System.out.println("Impossible de se connecter: " + e.toString());
  			   if (e.getCause() != null)
  			   {
  				   System.out.println("Exception: " + e.getCause().toString());
  			   }
  		   }
  	   })
  	   .thenAccept((unused) ->
  	   {
  		   System.out.println("Appuyer sur entrer pour arreter l'ecoute");
  	  	   try 
  		   {
  			   System.in.read();
  		   }
  		   catch (Exception e)
  		   {
  			   System.out.println("Lecture du clavier impossible : " + e.toString());
  		   }
  	   })
  	   .thenCompose((unused) ->
   	   {
   		   System.out.println("Unused : " + unused);
  	 	   return host.unregisterEventProcessor();
  	   })
  	   .exceptionally((e) ->
  	   {
  		   System.out.println("Impossible de se deconnecter : " + e.toString());
  		   if (e.getCause() != null)
  		   {
  			   System.out.println("Exception : " + e.getCause().toString());
  		   }
  		   return null;
  	   })
  	   .get();

         System.out.println("End of Receiver");
    }
}