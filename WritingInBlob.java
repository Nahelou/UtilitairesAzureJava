package cmt.cmtTest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.gson.JsonObject;

//Module d'ecriture d'un Json dans le blob Storage

public class WritingInBlob
{
    public static void write(JsonObject jsonPosition, String IdBalise) {
    	try {
			System.out.println( "START !" );
			System.out.println( "Objet Json à envoyer au blob Storage : "+ jsonPosition);
			String accountName = "MonAccountName";
			String accountKey = "MonAccountKey";
			
			StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
			String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);
			
			
			BlobServiceClient storageClient = new BlobServiceClientBuilder().endpoint(endpoint).credential(credential).buildClient();
			System.out.println( "BlobServiceClient OK" );
			
			//Creation d'un conteneur s'il n'existe pas et écrire un fichier json a l'interieur
			try {
				BlobContainerClient blobContainerClient = storageClient.getBlobContainerClient(IdBalise);
				blobContainerClient.create();

				System.out.println( "BlobContainerClient cree avec succes. Conteneur : " + IdBalise);
				
				InputStream dataStream = new ByteArrayInputStream(jsonPosition.toString().getBytes(StandardCharsets.UTF_8));

				BlockBlobClient blobClient = blobContainerClient.getBlobClient("ID" + IdBalise + "_" + Instant.now() + ".json" ).getBlockBlobClient();

				blobClient.upload(dataStream, jsonPosition.toString().length());
				System.out.println( "UPLOAD OK" );

				dataStream.close();
				return;
			}
			
			//recuperation du conteneur existant (idbalise) et ecriture d'un fichier json a l'interieur

			catch(Exception e){
				BlobContainerClient blobContainerClient = storageClient.getBlobContainerClient(IdBalise);

				System.out.println( "BlobContainerClient existant. Conteneur : " + IdBalise);
				
				InputStream dataStream = new ByteArrayInputStream(jsonPosition.toString().getBytes(StandardCharsets.UTF_8));

				BlockBlobClient blobClient = blobContainerClient.getBlobClient("ID" + IdBalise + "_" + Instant.now() + ".json" ).getBlockBlobClient();

				blobClient.upload(dataStream, jsonPosition.toString().length());
				System.out.println( "UPLOAD OK" );

				dataStream.close();
				return;				
			}

       } catch(Exception e){
    	   System.out.println(e.getMessage());
       }   
    }
}