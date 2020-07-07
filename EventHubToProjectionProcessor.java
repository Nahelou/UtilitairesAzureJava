package cmt.cmtTest;

import java.util.concurrent.ExecutionException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;

public class EventHubToProjectionProcessor implements IEventProcessor {

	private int checkpointBatchingCount = 0;

	// OnOpen is called when a new event processor instance is created by the host.
	@Override
	public void onOpen(PartitionContext context) throws Exception {
		System.out.println("Partition " + context.getPartitionId() + " is opening");
	}

	// OnClose is called when an event processor instance is being shut down.
	@Override
	public void onClose(PartitionContext context, CloseReason reason) throws Exception {
		System.out.println("Partition " + context.getPartitionId() + " is closing for reason " + reason.toString());
	}

	// onError is called when an error occurs in EventProcessorHost code that is
	// tied to this partition, such as a receiver failure.
	@Override
	public void onError(PartitionContext context, Throwable error) {
		System.out.println("Partition " + context.getPartitionId() + " onError: " + error.toString());
	}

	public static void wait(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	// onEvents is called when events are received on this partition of the Event
	// Hub.

	@Override
	public void onEvents(PartitionContext context, Iterable<EventData> events) throws Exception {
		DatabaseUtils databaseUtils = new DatabaseUtils();
		System.out.println("Partition " + context.getPartitionId() + " got event batch");
		int eventCount = 0;
		System.out.println("EVENT TO BACKEND CATCHED ...... SLEEPING FOR 5 seconds");
		wait(5000);
		System.out.println("INTERPOLATE POINT ON LINE AND SEND TO QUEUE ......");
		for (EventData data : events) {
			try {
				System.out.println("(Partition id , eventData offset, eventData sequenceNumber )("
						+ context.getPartitionId() + "," + data.getSystemProperties().getOffset() + ","
						+ data.getSystemProperties().getSequenceNumber() + "): "
						+ "Json ( to string ) de la position du train : " + new String(data.getBytes(), "UTF8"));
				String dataStream = new String(data.getBytes(), "UTF8");
				@SuppressWarnings("deprecation")
				JsonObject jsonObject = new JsonParser().parse(dataStream).getAsJsonObject();
				JsonObject jsonMap = (JsonObject) jsonObject;
				System.out.println("jsonMap : " + jsonMap);

				databaseUtils.exportDataToBdd(jsonMap);

				eventCount++;

				// Checkpointing persists the current position in the event stream for this
				// partition and means that the next
				// time any host opens an event processor on this event hub+consumer
				// group+partition combination, it will start
				// receiving at the event after this one.
				this.checkpointBatchingCount++;
				if ((checkpointBatchingCount % 5) == 0) {
					System.out.println(" Partition " + context.getPartitionId() + " checkpointing at "
							+ data.getSystemProperties().getOffset() + ","
							+ data.getSystemProperties().getSequenceNumber());
					context.checkpoint(data).get();
				}
			} catch (InterruptedException | ExecutionException e) {
				System.out.println("Processing fail pour l'event : " + e.getMessage());
			}
		}
		System.out.println("Partition " + context.getPartitionId() + " batch size was " + eventCount + " for host "
				+ context.getOwner());
	}

}
