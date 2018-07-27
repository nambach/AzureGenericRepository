package azure.component;

import java.util.List;

public class BootgridResponse<T> {
    private int current;
    private int rowCount;
    private int total;
    private List<T> rows;

    public BootgridResponse(int current, int rowCount, int total, List<T> rows) {
        this.current = current;
        this.rowCount = rowCount;
        this.total = total;
        this.rows = rows;
    }

    public BootgridResponse() {
    }

    public int getCurrent() {
        return current;
    }

    public void setCurrent(int current) {
        this.current = current;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public List<T> getRows() {
        return rows;
    }

    public void setRows(List<T> rows) {
        this.rows = rows;
    }
}
