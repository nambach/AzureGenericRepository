package azure;

public interface BlobService {

    String upload(String containerName, String blobName, byte[] data);
}
