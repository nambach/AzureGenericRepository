package azure.business.impl;

import azure.annotation.AzureTableName;
import azure.business.GenericBusiness;
import azure.component.BootgridResponse;
import azure.component.GenericEntity;
import azure.component.GenericModel;
import azure.impl.TableServiceImpl;
import azure.util.QueryUtils;
import util.GenericClassUtils;
import util.StringUtils;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static azure.constant.Constants.RESPONSE_CODE.*;

public class GenericBusinessImpl<M extends GenericModel<E>, E extends GenericEntity<M>> implements GenericBusiness<M> {

    private TableServiceImpl<E> tableService;

    private Class<M> modelClass;
    private Class<E> entityClass;

    public GenericBusinessImpl() {
        this.modelClass = GenericClassUtils.getGenericClass(this.getClass(), 0);
        this.entityClass = GenericClassUtils.getGenericClass(this.getClass(), 1);
        this.tableService = new TableServiceImpl<>(entityClass, getAzureTableName());

    }

    private String getAzureTableName() {
        // Get table name
        AzureTableName tableName = entityClass.getAnnotation(AzureTableName.class);
        return tableName.value();
    }

    @Override
    public int insert(M model) {
        if (tableService.getEntity(model.getPartitionKey(), model.getRowKey()) == null) {
            return tableService.insertOrReplace(model.toEntity())
                    ? CREATED
                    : INTERNAL_SERVER_ERROR;
        } else {
            return CONFLICT;
        }
    }

    @Override
    public int update(M model, String... properties) {
        E currentEntity = tableService.getEntity(model.getPartitionKey(), model.getRowKey());

        // Check existence
        if (currentEntity != null) {
            M currentModel = currentEntity.toModel();

            // Set properties to model
            for (String property : properties) {
                try {
                    Field field = modelClass.getDeclaredField(property);
                    field.setAccessible(true);
                    field.set(currentModel, field.get(model));
                } catch (NoSuchFieldException | IllegalAccessException ignored) {
                }
            }

            return tableService.insertOrReplace(currentModel.toEntity())
                    ? OK
                    : INTERNAL_SERVER_ERROR;
        } else {
            return NOT_FOUND;
        }
    }

    @Override
    public int updateWhole(M model) {
        E currentEntity = tableService.getEntity(model.getPartitionKey(), model.getRowKey());

        // Check existence
        if (currentEntity != null) {
            M currentModel = currentEntity.toModel();

            // Set properties to model - only properties that is NOT NULL
            for (Field field : modelClass.getDeclaredFields()) {
                try {
                    field.setAccessible(true);
                    Object fieldValue = field.get(model);
                    if (fieldValue == null) {
                        continue;
                    }
                    field.set(currentModel, fieldValue);
                } catch (IllegalAccessException ignored) {
                }
            }

            return tableService.insertOrReplace(currentModel.toEntity())
                    ? OK
                    : INTERNAL_SERVER_ERROR;
        } else {
            return NOT_FOUND;
        }
    }

    @Override
    public int remove(M model) {
        E entity = tableService.getEntity(model.getPartitionKey(), model.getRowKey());

        // Check existence
        if (entity != null) {
            return tableService.delete(entity) != null
                    ? OK
                    : INTERNAL_SERVER_ERROR;
        } else {
            return NOT_FOUND;
        }
    }

    @Override
    public M get(M model) {
        E entity = null;

        String partitionKey = model.getPartitionKey();
        String rowKey = model.getRowKey();

        if (partitionKey != null && rowKey != null) {
            entity = tableService.getEntity(partitionKey, rowKey);
        } else if (partitionKey == null && rowKey != null) {
            entity = tableService.getEntity(rowKey);
        }

        if (entity != null) {
            return entity.toModel();
        } else {
            return null;
        }
    }

    @Override
    public List<M> getAll(M sampleModel) {
        List<String> filters = new LinkedList<>();

        // Set properties to model - only properties that is NOT NULL
        for (Field field : modelClass.getDeclaredFields()) {
            try {
                field.setAccessible(true);

                // Get Key and Value of property
                Object fieldValue = field.get(sampleModel);
                if (fieldValue == null) {
                    continue;
                }
                String fieldName = StringUtils.capitalize(field.getName(), 0);

                // Add filter to list
                filters.add(QueryUtils.getEqualFilter(fieldName, fieldValue + ""));
            } catch (IllegalAccessException ignored) {
            }
        }

        // Combine filters
        String[] f = new String[filters.size()];
        String azureFilter = QueryUtils.combineFilters(filters.toArray(f));

        // Query from table service
        List<E> entities = tableService.query(sampleModel.getPartitionKey(), azureFilter);

        if (entities != null) {
            return entities.stream().map(E::toModel).collect(Collectors.toList());
        } else {
            return new LinkedList<>();
        }
    }

    @Override
    public List<M> getAll(String partitionKey, String equalConditions) {
        List<E> entities = tableService.searchAll(partitionKey, equalConditions, null);
        if (entities != null) {
            return entities.stream().map(E::toModel).collect(Collectors.toList());
        } else {
            return new LinkedList<>();
        }
    }

    @Override
    public BootgridResponse<M> getPage(int rowCount, int currentPage, String partitionKey, String tableServiceQueryFilter) {
        BootgridResponse<E> entities = tableService.searchPage(rowCount, currentPage, partitionKey, tableServiceQueryFilter);
        BootgridResponse<M> models = null;
        try {
            models = new BootgridResponse<>(
                    entities.getCurrent(),
                    entities.getRowCount(),
                    entities.getTotal(),
                    entities.getRows().stream().map(E::toModel).collect(Collectors.toList())
            );
        } catch (Exception ignored) {
        }
        return models;
    }
}
