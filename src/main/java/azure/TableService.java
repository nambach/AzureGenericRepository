package azure;

import azure.component.BootgridResponse;

import java.util.List;

public interface TableService<T> {

    boolean insertOrReplace(T entity);

    boolean insertOrMerge(T entity);

    boolean insertOrReplaceBatch(List<T> entities);

    boolean insertOrMergeBatch(List<T> entities);

    T delete(T entity);

    T getEntity(T entity);

    T getEntity(String partitionKey, String rowKey);

    T getEntity(String rowKey);

    List<T> query(String partitionKey, String azureFilter);

    List<T> searchAll();

    List<T> searchAll(String partitionKey);

    List<T> searchAll(String partitionKey, String equalConditions);

    List<T> searchAll(String partitionKey, String equalConditions, String queryFilter);

    BootgridResponse<T> searchPage(int rowCount, int currentPage, String partitionKey, String queryFilter);

    List<T> searchTop(int count, String partitionKey, String equalConditions);

    int count();
}
