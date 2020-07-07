package cmt.cmtTest;

import java.text.MessageFormat;
import java.util.concurrent.ExecutionException;

import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;

// FIXME Review1 - commentaire - Convertir les messages d'information en Log
public class EventHubListener {

	private static final String MESSAGE_ERROR = "Impossible de se connecter: {0} pour cause: {1}.";
	private static final String READ_MESSAGE_ERROR = "Lecture du clavier impossible {0} pour cause: {1}.";

	public void getEventHubs() throws InterruptedException, ExecutionException {

		EventProcessorHost host = AccessUtils.getEventProcessorhostStorage();
		System.out.println("Connexion a l'host : " + host.getHostName());

		host.registerEventProcessor(EventHubToBlobStorageProcessor.class, new EventProcessorOptions())//
				.whenComplete((unused, exception) -> {
					if (exception != null && exception.getCause() != null) {
						System.out.println(MessageFormat.format(MESSAGE_ERROR, exception.toString(),
								exception.getCause().toString()));
					}
				}).thenAccept((unused) -> {
					System.out.println("Appuyer sur entrer pour arreter l'ecoute");
					try {
						System.in.read();
					} catch (Exception exception) {
						System.out.println(MessageFormat.format(READ_MESSAGE_ERROR, exception.toString(),
								exception.getCause().toString()));
					}
				}).thenCompose((unused) -> {
					System.out.println("Deconnexion de l'event processor...");
					return host.unregisterEventProcessor();
				}).exceptionally((exception) -> {
					if (exception != null && exception.getCause() != null) {
						System.out.println(MessageFormat.format(MESSAGE_ERROR, exception.toString(),
								exception.getCause().toString()));
					}
					return null;
				}).get();

		System.out.println("End of Receiver");
	}
}