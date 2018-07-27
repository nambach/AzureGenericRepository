package azure.util;

import com.microsoft.azure.storage.table.TableQuery;

public class QueryUtils {
    public static String getEqualFilter(String columnName, String value) {
        return TableQuery.generateFilterCondition(
                columnName,
                TableQuery.QueryComparisons.EQUAL,
                value
        );
    }

    public static String combineFilters(String...filter) {
        String combinedFilter = "";
        if (filter.length < 2) {
            return null;
        }

        combinedFilter = TableQuery.combineFilters(
                filter[0],
                TableQuery.Operators.AND,
                filter[1]
        );

        if (filter.length > 2) {
            for (int i = 2; i < filter.length; i++) {
                combinedFilter = TableQuery.combineFilters(
                        combinedFilter,
                        TableQuery.Operators.AND,
                        filter[i]
                );
            }
        }

        return combinedFilter;
    }
}
