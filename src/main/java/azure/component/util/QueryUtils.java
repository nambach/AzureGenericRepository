package azure.component.util;

import com.microsoft.azure.storage.table.TableQuery;

import java.util.LinkedList;
import java.util.List;

public class QueryUtils {
    public static String getEqualFilter(String columnName, String value) {
        return TableQuery.generateFilterCondition(
                columnName,
                TableQuery.QueryComparisons.EQUAL,
                value
        );
    }

    public static String combineFilters(String...filter) {
        String combinedFilter;

        filter = removeNull(filter);

        if (filter.length == 0) {
            return null;
        }

        if (filter.length == 1) {
            return filter[0];
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

    private static String[] removeNull(String[] arr) {
        List<String> list = new LinkedList<>();

        for (String s : arr) {
            if (s != null) {
                list.add(s);
            }
        }

        String[] newArr = new String[list.size()];
        return list.toArray(newArr);
    }
}
