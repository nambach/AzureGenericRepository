package azure.component.constant;

public class Constants {

    public static final String AZURE_ACC_NAME = "AZURE_STORAGE_ACCOUNT_NAME";
    public static final String AZURE_ACC_KEY = "AZURE_STORAGE_ACCOUNT_KEY";
    public static final String PARTITION_KEY = "PartitionKey";
    public static final String ROW_KEY = "RowKey";
    public static final int MAX_QUERY_COUNT = 1000;

    public static final class RESPONSE_CODE {
        public static final int OK = 200;
        public static final int CREATED = 201;

        public static final int NOT_FOUND = 404;
        public static final int CONFLICT = 409;

        public static final int INTERNAL_SERVER_ERROR = 500;
    }

}
