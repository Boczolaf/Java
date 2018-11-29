package dataframe;

import dataframe.types.Type;
import dataframe.values.COOValue;
import dataframe.values.Value;

import java.util.ArrayList;

public class SingleColumn {
    private int iscoo = 2;
    private Type typeofmycolumn;
    private String nameofmycolumn;
    public ArrayList<COOValue> listofcoovalues;
    public ArrayList<Value> listofvalues;

    public SingleColumn(String name, Type type) {
        typeofmycolumn = type;
        nameofmycolumn = name;
        listofvalues = new ArrayList<Value>();
        listofcoovalues = new ArrayList<COOValue>();
    }

    public void SetIsCoo(int h) {
        if (h == 0 || h == 1 || h == 2) {
            iscoo = h;
        }
    }

    public Type GetType() {
        return typeofmycolumn;
    }

    public String GetName() {
        return nameofmycolumn;
    }

    public int GetSize() {
        return listofvalues.size();
    }

    public SingleColumn(SingleColumn OtherColumn) {
        typeofmycolumn = OtherColumn.GetType();
        nameofmycolumn = OtherColumn.GetName();
        listofvalues = OtherColumn.listofvalues;
    }

    public void SetValue(int myindex, Value value1) {
        if (iscoo == 0) {
            listofvalues.set(myindex, value1);
        }
        if (iscoo == 1) {
            listofcoovalues.set(myindex, (COOValue) value1);
        }
    }

    public void addValue(Value value1) {
        if (iscoo == 2) {
            iscoo = 0;
        }
        if (iscoo == 1) {
            throw new RuntimeException("You can't overdrive the column that was once set as a nonCoovalue!!!");
        } else {
            listofvalues.add(value1);
        }

    }

    public void AddCOOValue(Value value1, int index) {
        if (iscoo == 2) {
            iscoo = 1;
        }
        if (iscoo == 0) {
            throw new RuntimeException("You can't overdrive the column that was once set as a Coovalue!!!");
        } else {
            listofcoovalues.add(new COOValue(index, value1));
        }
    }

}
