package dataframe.Exceptions;

public class TriedToDoInvalidColOperation extends RuntimeException {
    public String col1;
    public String col2;

    public TriedToDoInvalidColOperation(String colname1, String colname2) {
        super();
        col1 = colname1;
        col2 = colname2;
    }
}
