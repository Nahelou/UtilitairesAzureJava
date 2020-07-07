package cmt.cmtTest;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

public class BackendJsonSender {

	public void sendEventToEventHub(JsonObject pos, EventHubType eventHub) throws EventHubException, IOException {

		Gson gson = new GsonBuilder().create();
		ConnectionStringBuilder connStr = AccessUtils.getEventHubConnectionString(eventHub);

		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

		EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);

		try {
			byte[] payloadBytes = gson.toJson(pos).getBytes(Charset.defaultCharset());
			EventData eventToSend = EventData.create(payloadBytes);
			ehClient.sendSync(eventToSend);
			System.out.println(Instant.now() + ": Send Complete...");

		} finally {
			ehClient.closeSync();
			executorService.shutdown();
		}

		System.out.println(connStr);
		System.out.println("End of Sender");
	}

}
