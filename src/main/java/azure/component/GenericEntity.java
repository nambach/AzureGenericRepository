package azure.component;

import com.microsoft.azure.storage.table.TableServiceEntity;

public abstract class GenericEntity<T> extends TableServiceEntity {
    public abstract T toModel();
}
