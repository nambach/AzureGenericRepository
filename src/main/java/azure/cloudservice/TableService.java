package azure.cloudservice;

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

    List<T> queryAll();

    List<T> query(String partitionKey, String azureFilter);

    BootgridResponse<T> queryPage(int rowCount, int currentPage, String partitionKey, String azureFilter);

    List<T> queryTop(int count, String partitionKey, String azureFilter);

    int count();
}
