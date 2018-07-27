package azure.component;

import com.microsoft.azure.storage.table.TableServiceEntity;

public abstract class GenericModel<T extends TableServiceEntity> {
    public abstract String getPartitionKey();
    public abstract String getRowKey();
    public abstract T toEntity();
}
