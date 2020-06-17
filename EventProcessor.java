
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;

public class EventProcessor implements IEventProcessor
{
	private int checkpointBatchingCount = 0;

	@Override
    public void onOpen(PartitionContext context) throws Exception
    {
    	System.out.println("Partition " + context.getPartitionId() + " is opening");
    }

	@Override
    public void onClose(PartitionContext context, CloseReason reason) throws Exception
    {
        System.out.println("Partition " + context.getPartitionId() + " is closing for reason " + reason.toString());
    }

	@Override
	public void onError(PartitionContext context, Throwable error)
	{
		System.out.println("Partition " + context.getPartitionId() + " onError: " + error.toString());
	}

	@Override
    public void onEvents(PartitionContext context, Iterable<EventData> events) throws Exception
    {
        System.out.println("Partition " + context.getPartitionId() + " got event batch");
        int eventCount = 0;
        for (EventData data : events)
        {
        	try
        	{
                System.out.println("(Partition id , eventData offset, eventData sequenceNumber )(" + context.getPartitionId() + "," + data.getSystemProperties().getOffset() + "," +
                 		data.getSystemProperties().getSequenceNumber() + "): " + "Json ( to string ) de la position du train : " + new String(data.getBytes(), "UTF8"));
                String dataStream = new String(data.getBytes(), "UTF8");
                 @SuppressWarnings("deprecation")
				JsonObject jsonObject = new JsonParser().parse(dataStream).getAsJsonObject();
                JsonObject jsonMap = (JsonObject) jsonObject.get("map");
                System.out.println("Data Stream : " + dataStream);
                System.out.println("Data Stream To Json : " + jsonMap.get("X"));
                BddActions.exportDataToBdd(BddActions.insertTemporaryPosition(jsonMap.get("X").getAsDouble(), jsonMap.get("Y").getAsDouble(), jsonMap.get("IDBALISE").getAsString() ));
                 
                eventCount++;

                 this.checkpointBatchingCount++;
                 if ((checkpointBatchingCount % 5) == 0)
                 {
                 	System.out.println(" Partition " + context.getPartitionId() + " checkpointing at " +
                			data.getSystemProperties().getOffset() + "," + data.getSystemProperties().getSequenceNumber());
                 	context.checkpoint(data).get();
                 }
        	}
        	catch (Exception e)
        	{
        		System.out.println("Processing fail pour l'event : " + e.toString());
        	}
        }
        System.out.println("Partition " + context.getPartitionId() + " batch size was " + eventCount + " for host " + context.getOwner());
    }
	
}


