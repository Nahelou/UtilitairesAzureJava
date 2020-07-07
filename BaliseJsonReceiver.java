package cmt.cmtTest;

import java.util.concurrent.ExecutionException;

import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;

public class BaliseJsonReceiver {

	public void getBaliseJson() throws InterruptedException, ExecutionException {

		EventProcessorHost host = AccessUtils.getEventProcessorhost();
		System.out.println("Connexion a l'host : " + host.getHostName());
		EventProcessorOptions options = new EventProcessorOptions();

		host.registerEventProcessor(EventHubToProjectionProcessor.class, options).whenComplete((unused, e) -> {
			if (e != null) {
				System.out.println("Impossible de se connecter: " + e.toString());
				if (e.getCause() != null) {
					System.out.println("Exception: " + e.getCause().toString());
				}
			}
		}).thenAccept((unused) -> {
			System.out.println("Appuyer sur entrer pour arreter l'ecoute");
			try {
				System.in.read();
			} catch (Exception e) {
				System.out.println("Lecture du clavier impossible : " + e.toString());
			}
		}).thenCompose((unused) -> {
			System.out.println("Unused : " + unused);
			return host.unregisterEventProcessor();
		}).exceptionally((e) -> {
			System.out.println("Impossible de se deconnecter : " + e.toString());
			if (e.getCause() != null) {
				System.out.println("Exception : " + e.getCause().toString());
			}
			return null;
		}).get();

		System.out.println("End of Receiver");
	}
}