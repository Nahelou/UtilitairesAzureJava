package cmt.cmtTest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class BlobWriter {

	final Gson gson;

	public BlobWriter() {
		gson = new GsonBuilder().create();
	}

	public void write(JsonObject jsonPosition, String idBalise) {
		try {
			System.out.println("START !");
			System.out.println("Objet Json a envoyer au blob Storage : " + jsonPosition);
			String accountName ='YOURACCOUNTNAME';
			String accountKey = "YOURACCOUNTKEY";
			StorageCredentialsAccountAndKey StorageCredentialsAccountAndKey = new StorageCredentialsAccountAndKey(
					accountName, accountKey);

			System.out.println("BlobServiceClient OK");

			CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(StorageCredentialsAccountAndKey, true);
			CloudBlobClient cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
			CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference("cmttest");

			cloudBlobContainer.createIfNotExists();

			CloudAppendBlob appendBlob = cloudBlobContainer
					.getAppendBlobReference(idBalise + "/" + java.time.LocalDate.now() + "/appendblob.json");
			byte[] payloadBytes = gson.toJson(jsonPosition).getBytes(Charset.defaultCharset());

			InputStream dataStream = new ByteArrayInputStream(jsonPosition.toString().getBytes(StandardCharsets.UTF_8));

			try {
				System.out.println(" TRY ");

				System.out.println("BlobContainerClient cree avec succes. Conteneur : " + idBalise);

				appendBlob.appendText(", ");
				appendBlob.appendFromByteArray(payloadBytes, 0, jsonPosition.toString().length());

				System.out.println("\t\tSuccessfully created the append blob and appended data to it.");

				System.out.println("UPLOAD OK");

				dataStream.close();
				return;
			} catch (Exception e) {

				System.out.println(" CATCH ");

				appendBlob.createOrReplace();
				appendBlob.appendText("{");
				appendBlob.appendFromByteArray(payloadBytes, 0, jsonPosition.toString().length());

				System.out.println("UPLOAD OK");

				dataStream.close();
				return;
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}