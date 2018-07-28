package azure.cloudservice.impl;

import azure.cloudservice.TableService;
import azure.component.BootgridResponse;
import azure.component.GenericEntity;
import azure.component.annotation.AzureTableName;
import azure.component.util.QueryUtils;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.*;
import util.GenericClassUtils;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static azure.component.constant.Constants.*;

public class TableServiceImpl<T extends GenericEntity> implements TableService<T> {

    private CloudTable cloudTable;
    private String tableName;
    private Class<T> entityClass;

    public TableServiceImpl() {
        setEntityClass();
        setTableName();
        setCloudTable();
    }

    public TableServiceImpl(Class<T> entityClass, String tableName) {
        this.entityClass = entityClass;
        this.tableName = tableName;
        setCloudTable();
    }

    private void setEntityClass() {
        this.entityClass = GenericClassUtils.getGenericClass(this.getClass(), 0);
    }

    private void setTableName() {
        // Get table name
        AzureTableName tableName = entityClass.getAnnotation(AzureTableName.class);
        this.tableName = tableName.value();
    }

    private void setCloudTable() {
        String name = System.getenv(AZURE_ACC_NAME);
        String key = System.getenv(AZURE_ACC_KEY);

        String storageConnectionString =
                "DefaultEndpointsProtocol=https;" +
                        "AccountName=" + name +";" +
                        "AccountKey=" + key + ";" +
                        "TableEndpoint=https://" + name + ".table.core.windows.net;";

        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();

            // Create a cloud table object for the table.
            cloudTable = tableClient.getTableReference(tableName);
            if (!cloudTable.exists()) {
                cloudTable.create();
            }

        } catch (URISyntaxException | InvalidKeyException | StorageException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean insertOrReplace(T entity) {
        try {
            // Create an operation to add the new customer to the people table.
            TableOperation insertCustomer = TableOperation.insertOrReplace(entity);

            // Submit the operation to the table service.
            cloudTable.execute(insertCustomer);

            return true;
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean insertOrMerge(T entity) {
        try {
            // Create an operation to add the new customer to the people table.
            TableOperation insertCustomer = TableOperation.insertOrMerge(entity);

            // Submit the operation to the table service.
            cloudTable.execute(insertCustomer);

            return true;
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean insertOrReplaceBatch(List<T> entities) {
        try {
            TableBatchOperation batchOperation = new TableBatchOperation();

            entities.forEach(batchOperation::insertOrReplace);

            // Submit the operation to the table service.
            cloudTable.execute(batchOperation);

            return true;
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean insertOrMergeBatch(List<T> entities) {
        try {
            TableBatchOperation batchOperation = new TableBatchOperation();

            entities.forEach(batchOperation::insertOrMerge);

            // Submit the operation to the table service.
            cloudTable.execute(batchOperation);

            return true;
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public T delete(T entity) {
        try
        {
            // Create an operation to retrieve the entity with partition key of "Smith" and row key of "Jeff".
            TableOperation retrieveOperation = TableOperation.retrieve(entity.getPartitionKey(), entity.getRowKey(), entityClass);

            // Retrieve the entity with partition key of "Smith" and row key of "Jeff".
            T toDeleteEntity =
                    cloudTable.execute(retrieveOperation).getResultAsType();

            // Create an operation to delete the entity.
            TableOperation deleteOperation = TableOperation.delete(toDeleteEntity);

            // Submit the delete operation to the table service.
            cloudTable.execute(deleteOperation);

            return toDeleteEntity;
        }
        catch (Exception e)
        {
            // Output the stack trace.
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public T getEntity(T entity) {
        String partitionKey = entity.getPartitionKey();
        String rowKey = entity.getRowKey();

        if (partitionKey != null && rowKey != null) {
            return getEntity(partitionKey, rowKey);
        } else if (partitionKey == null && rowKey != null){
            return getEntity(rowKey);
        } else {
            return null;
        }
    }

    @Override
    public T getEntity(String partitionKey, String rowKey) {
        if (partitionKey == null || rowKey == null) {
            return null;
        }
        try {
            // Create an operation to retrieve the entity with partition key and row key
            TableOperation retrieveOperation = TableOperation.retrieve(partitionKey, rowKey, entityClass);

            // Retrieve the entity with partition key and row key
            return cloudTable.execute(retrieveOperation).getResultAsType();
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Get an entity (only if rowKeys are distinct!!)
     *
     * @param rowKey entity's rowKey
     * @return entity
     */
    @Override
    public T getEntity(String rowKey) {
        if (rowKey == null) {
            return null;
        }
        try {

            // Prepare partitionKey filter
            String rowKeyFilter = QueryUtils.getEqualFilter(ROW_KEY, rowKey);

            // Specify a partition query
            TableQuery<T> rowKeyQuery =
                    TableQuery.from(entityClass)
                            .where(rowKeyFilter);

            // Collect entities.
            List<T> list = new ArrayList<>();
            ResultContinuation token = null;

            do {
                ResultSegment<T> queryResult = cloudTable.executeSegmented(rowKeyQuery, token);
                list.addAll(queryResult.getResults());
                token = queryResult.getContinuationToken();
            } while (token != null);

            return !list.isEmpty() ? list.get(0) : null;
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<T> queryAll() {
        try {
            ResultContinuation token = null;
            List<T> list = new ArrayList<>();

            do {
                ResultSegment<T> queryResult = cloudTable.executeSegmented(TableQuery.from(entityClass), token);
                list.addAll(queryResult.getResults());
                token = queryResult.getContinuationToken();
            } while (token != null);

            return list;
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    @Override
    public List<T> query(String partitionKey, String azureFilter) {
        try {

            // Specify a combo query
            TableQuery<T> query = createQuery(partitionKey, azureFilter);

            // Collect entities.
            List<T> list = new ArrayList<>();
            ResultContinuation token = null;

            do {
                ResultSegment<T> queryResult = cloudTable.executeSegmented(query, token);
                list.addAll(queryResult.getResults());
                token = queryResult.getContinuationToken();
            } while (token != null);

            return list;
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    @Override
    public BootgridResponse<T> queryPage(int rowCount, int currentPage, String partitionKey, String azureFilter) {
        try {

            // Specify a combo query
            TableQuery<T> query = createQuery(partitionKey, azureFilter);

            // Collect entities.
            List<T> list = new ArrayList<>();
            ResultContinuation token = null;

            do {
                ResultSegment<T> queryResult = cloudTable.executeSegmented(query, token);
                list.addAll(queryResult.getResults());
                token = queryResult.getContinuationToken();
            } while (token != null);

            // Extract entities by pages
            List<T> rows = new LinkedList<>();
            int startIndex = (currentPage - 1) * rowCount;
            int endIndex = currentPage * rowCount - 1;

            if (startIndex > list.size() - 1) {
                // last index = 5
                int lastPage = list.size() / rowCount;
                if (list.size() % rowCount == 0) {
                    lastPage--;
                }

                startIndex = lastPage * rowCount;

                for (int i = startIndex; i < list.size(); i++) {
                    rows.add(list.get(i));
                }

                // Update the page number (last page is calculated in programming index)
                currentPage = lastPage + 1;
            } else if (endIndex > list.size() - 1) {
                for (int i = startIndex; i < list.size(); i++) {
                    rows.add(list.get(i));
                }
            } else {
                for (int i = startIndex; i <= endIndex; i++) {
                    rows.add(list.get(i));
                }
            }

            return new BootgridResponse<>(currentPage, rowCount, list.size(), rows);
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return new BootgridResponse<>(0, 0, 0, Collections.emptyList());
        }
    }

    @Override
    public List<T> queryTop(int count, String partitionKey, String azureFilter) {
        List<T> list = new ArrayList<>();

        // Create TableQuery object
        TableQuery<T> query = createQuery(partitionKey, azureFilter);

        try {
            if (count <= 0) {
                return list;
            } else if (count <= MAX_QUERY_COUNT) {
                query = query.take(count);
                cloudTable.execute(query).forEach(list::add);

            } else {
                ResultContinuation token = null;

                do {
                    ResultSegment<T> queryResult = cloudTable.executeSegmented(query, token);
                    list.addAll(queryResult.getResults());
                    token = queryResult.getContinuationToken();
                    count -= MAX_QUERY_COUNT;
                } while (count > MAX_QUERY_COUNT && token != null);

                if (token != null) {
                    query = query.take(count);
                    ResultSegment<T> queryResult = cloudTable.executeSegmented(query, token);
                    list.addAll(queryResult.getResults());
                }
            }
            return list;
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    @Override
    public int count() {
        try {
            String column[] = {PARTITION_KEY};
            TableQuery<T> query = TableQuery.from(entityClass)
                    .select(column);

            ResultContinuation token = null;
            List<T> list = new ArrayList<>();

            do {
                ResultSegment<T> queryResult = cloudTable.executeSegmented(query, token);
                list.addAll(queryResult.getResults());
                token = queryResult.getContinuationToken();
            } while (token != null);

            return list.size();
        } catch (Exception e) {
            // Output the stack trace.
            e.printStackTrace();
            return 0;
        }
    }

    private TableQuery<T> createQuery(String partitionKey, String azureFilter) {

        // Generate PartitionKey filter
        String partitionFilter = null;
        if (partitionKey != null) {
            partitionFilter = QueryUtils.getEqualFilter(PARTITION_KEY, partitionKey);
        }

        // Combine all filters
        String filter = QueryUtils.combineFilters(partitionFilter, azureFilter);

        // Specify a combo query
        TableQuery<T> query = TableQuery.from(entityClass);
        if (filter != null) {
            query.where(filter);
        }

        return query;
    }
}
