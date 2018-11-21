package dataframe;

import java.util.ArrayList;
import java.lang.*;

public class SingleColumn {
    private int IsCoo=2;
    private  String TypeOfMyColumn, NameOfMyColumn;
    public ArrayList<COOValue> ListOfCoovalues;
    public ArrayList<Value> ListOfValues;
    public SingleColumn(String Name,String Type){
        TypeOfMyColumn=Type;
        NameOfMyColumn=Name;
        ListOfValues = new ArrayList<Value>();
        ListOfCoovalues = new ArrayList<COOValue>();
    }
    public void SetIsCoo(int h){
        if(h==0||h==1||h==2){
            IsCoo=h;
        }
    }
    public String GetType(){
        return TypeOfMyColumn;
    }
    public String GetName(){
        return NameOfMyColumn;
    }
    public int GetSize(){
        return ListOfValues.size();
    }
    public SingleColumn(SingleColumn OtherColumn){
        TypeOfMyColumn = OtherColumn.GetType();
        NameOfMyColumn = OtherColumn.GetName();
        ListOfValues = OtherColumn.ListOfValues;
    }
    public void SetValue(int MyIndex, Value Value1){
        if(IsCoo==0) {
            ListOfValues.set(MyIndex, Value1);
        }
        if(IsCoo==1){
            ListOfCoovalues.set(MyIndex,(COOValue) Value1);
        }
    }
    public void addValue(Value Value1){
        if(IsCoo==2){
            IsCoo=0;
        }
        if(IsCoo==1){
            throw new RuntimeException("You can't overdrive the column that was once set as a nonCoovalue!!!");
        }
        else {
            ListOfValues.add(Value1);
        }

    }
    public void AddCOOValue(Value Value1,int Index){
        if(IsCoo==2){
            IsCoo=1;
        }
        if(IsCoo==0){
            throw new RuntimeException("You can't overdrive the column that was once set as a Coovalue!!!");
        }
        else {
            ListOfCoovalues.add(new COOValue(Index, Value1));
        }
    }

}
