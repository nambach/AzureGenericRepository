package azure.business;

import azure.component.BootgridResponse;

import java.util.List;

public interface GenericBusiness<T> {

    int insert(T model);

    int update(T model, String... properties);

    int updateWhole(T model);

    int remove(T model);

    T get(T model);

    List<T> getAll(T sampleModel);

    List<T> getAll(String partitionKey, String equalConditions);

    BootgridResponse<T> getPage(int rowCount, int currentPage, String partitionKey, String tableServiceQueryFilter);
}
