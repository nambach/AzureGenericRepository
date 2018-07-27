package azure.impl;

import azure.BlobService;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static azure.constant.Constants.AZURE_ACC_KEY;
import static azure.constant.Constants.AZURE_ACC_NAME;

public class BlobServiceImpl implements BlobService {

    private CloudBlobClient blobClient;

    public BlobServiceImpl() {
        setCloudTable();
    }

    private void setCloudTable() {
        String name = System.getenv(AZURE_ACC_NAME);
        String key = System.getenv(AZURE_ACC_KEY);

        String storageConnectionString =
                "DefaultEndpointsProtocol=https;" +
                        "AccountName=" + name + ";" +
                        "AccountKey=" + key + ";" +
                        "TableEndpoint=https://" + name + ".table.core.windows.net;";

        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

            // Create the blob client.
            blobClient = storageAccount.createCloudBlobClient();
        } catch (URISyntaxException | InvalidKeyException e) {
            e.printStackTrace();
        }
    }

    private CloudBlobContainer getBlobContainer(String containerName) {
        try {
            CloudBlobContainer container = blobClient.getContainerReference(containerName);

            // Create the container if it does not exist with public access.
            container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());

            return container;
        } catch (URISyntaxException | StorageException e) {
            return null;
        }
    }

    public String upload(String containerName, String blobName, byte[] data) {
        CloudBlobContainer container = getBlobContainer(containerName);

        if (container != null) {
            try {
                CloudBlockBlob blob = container.getBlockBlobReference(blobName);

                InputStream inputStream = new ByteArrayInputStream(data);
                blob.upload(inputStream, inputStream.available());

                return blob.getUri().toString();
            } catch (URISyntaxException | StorageException | IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }
}
