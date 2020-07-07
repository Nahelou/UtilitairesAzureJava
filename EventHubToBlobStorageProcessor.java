package cmt.cmtTest;

import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.Iterator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;

public class EventHubToBlobStorageProcessor implements IEventProcessor {

	private static final String PARTITION_CHECKPOINT = "Partition {0} checkpointing at {1} , {2}.";
	private static final String PARTITION_SIZE = "Partition {0} batch size was {1} for host {2}.";

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
		System.out.println("Partition " + context.getPartitionId() + " got event batch");
		int eventCount = 0;
		System.out.println("EVENT TO STORAGE CATCHED ...... SLEEPING FOR 5 seconds");

		wait(10000);
		System.out.println("PROCESS TO STORAGE......");
		for (EventData data : events) {
			try {
				eventToBlobStorage(data);
				eventCount++;
				System.out.println(MessageFormat.format(PARTITION_CHECKPOINT, context.getPartitionId(),
						data.getSystemProperties().getOffset(), data.getSystemProperties().getSequenceNumber()));
				context.checkpoint(data).get();
			} catch (Exception e) {
				System.out.println("Processing fail pour l'event : " + e.toString());
			}
		}
		System.out.println(
				MessageFormat.format(PARTITION_SIZE, context.getPartitionId(), eventCount, context.getOwner()));
	}

	@SuppressWarnings("deprecation")
	public void eventToBlobStorage(EventData event) throws UnsupportedEncodingException {
		BlobWriter blobWriter = new BlobWriter();
		String dataStream = new String(event.getBytes(), "UTF8");
		JsonObject jsonObject = new JsonParser().parse(dataStream).getAsJsonObject();
		JsonObject jsonMap = (JsonObject) jsonObject;
		JsonArray jsonArray = (JsonArray) jsonMap.get("features");
		Iterator<JsonElement> iterator = jsonArray.iterator();

		while (iterator.hasNext()) {
			JsonObject jsonObjectFeatures = (JsonObject) iterator.next();
			JsonObject properties = (JsonObject) jsonObjectFeatures.get("properties");
			blobWriter.write(jsonMap, properties.get("idbalise").toString().replaceAll("\"", ""));
		}
	}
}
