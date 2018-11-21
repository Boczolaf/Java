package dataframe;

public class TriedToDoInvalidColOperation extends RuntimeException {
    public String Col1;
    public String Col2;
    TriedToDoInvalidColOperation(String ColName1,String ColName2){
        super();
        Col1=ColName1;
        Col2=ColName2;
    }
}
