package dataframe;

import javax.naming.Name;
import java.util.ArrayList;
import java.util.Objects;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.io.FileReader;


public class DataFrame implements groupby {
    private ArrayList<SingleColumn> MyDataFrame = new ArrayList<SingleColumn>();

    public DataFrame() {
    }

    public String SpecialSnowflake;

    public DataFrame(String[] NamesOfColumns, String[] TypesOfColumns) {
        if (NamesOfColumns.length != TypesOfColumns.length) {
            System.out.println("Wrong input");
            return;
        }
        for (int i = 0; i < NamesOfColumns.length; ++i) {
            MyDataFrame.add(new SingleColumn(NamesOfColumns[i], TypesOfColumns[i]));
        }

    }

    public int Size() {
        return MyDataFrame.get(0).ListOfValues.size();
    }

    public DataFrame(SingleColumn[] Columns) {
        for (int i = 0; i < Columns.length; ++i) {
            MyDataFrame.add(Columns[i]);
            if (MyDataFrame.get(0).ListOfValues.size() != MyDataFrame.get(i).ListOfValues.size())
                throw new RuntimeException("Wrong length of column");
        }
    }

    public void print() {
        for (int i = 0; i < this.Size(); i++) {
            for (SingleColumn k : MyDataFrame) {
                System.out.print(k.ListOfValues.get(i));
                System.out.print(" ");
            }
            System.out.print("\n");
        }
    }

    public void add(Value[] Values) {
        if (Values.length != MyDataFrame.size()) {
            throw new RuntimeException("Invalid length of input!!!");
        }
        int iterator = 0;
        for (SingleColumn k : MyDataFrame) {
            k.ListOfValues.add(Values[iterator++]);
        }
    }

    public void add(SingleColumn Values) {
        if (Values.GetSize() != MyDataFrame.get(0).GetSize()) {
            throw new RuntimeException("Invalid length of input!!!");
        }
        MyDataFrame.add(MyDataFrame.size(), Values);

    }

    public SingleColumn get(String Colname) {
        for (SingleColumn k : MyDataFrame) {
            if (Objects.equals(k.GetName(), Colname)) {
                return k;
            }
        }
        throw new RuntimeException("Column not found!");
    }

    public DataFrame get(String[] Cols, boolean Copy) {
        SingleColumn[] Tab = new SingleColumn[Cols.length];
        for (int i = 0; i < Tab.length; ++i) {
            if (!Copy) {
                Tab[i] = this.get(Cols[i]);
            } else { //deep copy
                Tab[i] = new SingleColumn(get(Cols[i]));
            }
        }
        DataFrame dataFrame = new DataFrame(Tab);
        return dataFrame;

    }

    public DataFrame iloc(int from, int to) {
        if (from < 0 || from >= MyDataFrame.size())
            throw new IndexOutOfBoundsException("No such index: " + from);

        else if (to < 0 || to >= MyDataFrame.size())
            throw new IndexOutOfBoundsException("No such index: " + to);

        else if (to < from)
            throw new IndexOutOfBoundsException("unable to create range from " + from + " to " + to);

        int size = MyDataFrame.size();
        String[] names = new String[size];
        String[] types = new String[size];
        for (int i = 0; i < size; ++i) {
            names[i] = MyDataFrame.get(i).GetName();
            types[i] = MyDataFrame.get(i).GetType();
        }
        DataFrame NewDataFrame = new DataFrame(names, types);
        Value[] tab = new Value[MyDataFrame.size()];
        for (int i = from; i <= to; ++i) {
            for (int j = 0; j < tab.length; j++) {
                tab[j] = MyDataFrame.get(j).ListOfValues.get(i);
            }
            NewDataFrame.add(tab);

        }
        return NewDataFrame;
    }

    public ArrayList<SingleColumn> GetArray() {
        return MyDataFrame;
    }

    public DataFrame iloc(int i) {
        return iloc(i, i);

    }
//to do

    public DataFrame(String NameOfFile, String[] NamesOfCols,String[] TypesOfCols, boolean Header)
            { try(BufferedReader br = new BufferedReader(new FileReader(NameOfFile))) {
                String temp;
                String[] strLine;
                if(Header==true){
                    temp = br.readLine();
                    strLine = temp.split(",");
                    for(int i=0;i<NamesOfCols.length;i++){
                        this.MyDataFrame.add(new SingleColumn(strLine[i],TypesOfCols[i]));
                    }
                }
                else {
                    for(int i=0;i<NamesOfCols.length;i++){
                        this.MyDataFrame.add(new SingleColumn(NamesOfCols[i],TypesOfCols[i]));
                    }
                }
                while ((temp = br.readLine()) != null) {




                        strLine = temp.split(",");
                        for(int i=0;i<NamesOfCols.length;i++){
                            this.MyDataFrame.add(new SingleColumn(NamesOfCols[i],NamesOfCols[i]));


                }

            }}catch (IOException ex){
                throw new RuntimeException(ex);
            }



            }

    void SetSpecialSnowflake(String v) {
        SpecialSnowflake = v;
    }

    DataFrame groupby(String id) {
        SingleColumn[] Cols = new SingleColumn[Size()];
        int h = 0;
        for (SingleColumn k : GetArray()) {
            Cols[h] = new SingleColumn(k);
        }
        DataFrame returnframe = new DataFrame(Cols);
        returnframe.SetSpecialSnowflake(id);
        return returnframe;
    }

    public DataFrame max() {
        Value max = GetArray().get(0).ListOfValues.get(0);
        SingleColumn[] NewColumns = new SingleColumn[GetArray().size()];
        int h = 0;
        for (SingleColumn k : GetArray()) {
            if (SpecialSnowflake == null || SpecialSnowflake != k.GetName()) {
                max = GetArray().get(h).ListOfValues.get(0);
                for (int i = 0; i < k.GetSize(); i++) {
                    if (k.ListOfValues.get(i).gte(max)) {
                        max = k.ListOfValues.get(i);
                    }
                }
                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].ListOfValues.add(max);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }


    public DataFrame min() {
        Value min = GetArray().get(0).ListOfValues.get(0);
        SingleColumn[] NewColumns = new SingleColumn[GetArray().size()];
        int h = 0;
        for (SingleColumn k : GetArray()) {
            if (SpecialSnowflake == null || SpecialSnowflake != k.GetName()) {
                min = GetArray().get(h).ListOfValues.get(0);
                for (int i = 0; i < k.GetSize(); i++) {
                    if (k.ListOfValues.get(i).lte(min)) {
                        min = k.ListOfValues.get(i);
                    }
                }
                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].ListOfValues.add(min);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }


    public DataFrame mean() {
        Value mean;
        SingleColumn[] NewColumns = new SingleColumn[GetArray().size()];
        int h = 0;
        for (SingleColumn k : GetArray()) {
            if (SpecialSnowflake == null || SpecialSnowflake != k.GetName()) {

                mean = GetArray().get(h).ListOfValues.get(0);

                for (int i = 1; i < k.GetSize(); i++) {
                    mean = mean.add(k.ListOfValues.get(i));
                }

                mean = mean.div(new IntValue(k.GetSize()));

                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].ListOfValues.add(mean);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }


    public DataFrame std() {
        DataFrame newmean = this.mean();
        Value v = new IntValue(this.Size());
        SingleColumn[] NewColumns = new SingleColumn[GetArray().size()];
        Value Returnv = new IntValue(0);
        int h = 0;
        for (SingleColumn k : GetArray()) {
            if (SpecialSnowflake == null || SpecialSnowflake != k.GetName()) {
                for (int i = 0; i < k.GetSize(); i++) {
                    Returnv = Returnv.add((k.ListOfValues.get(i).sub(newmean.GetArray().get(h).ListOfValues.get(0))).pow(new IntValue(2)));
                }
                Returnv = Returnv.div(v);
                Returnv = Returnv.pow(new DblValue(0.5));
                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].ListOfValues.add(Returnv);
                Returnv = new IntValue(0);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }


    public DataFrame sum() {
        Value sum;
        SingleColumn[] NewColumns = new SingleColumn[GetArray().size()];
        int h = 0;
        for (SingleColumn k : GetArray()) {
            if (SpecialSnowflake == null || SpecialSnowflake != k.GetName()) {
                sum = GetArray().get(h).ListOfValues.get(0);
                for (int i = 1; i < k.GetSize(); i++) {
                    sum = sum.add(k.ListOfValues.get(i));
                }
                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].ListOfValues.add(sum);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }


    public DataFrame var() {
        DataFrame newmean = this.mean();
        Value v = new IntValue(this.Size());
        SingleColumn[] NewColumns = new SingleColumn[GetArray().size()];
        Value Returnv = new IntValue(0);
        int h = 0;
        for (SingleColumn k : GetArray()) {
            if (SpecialSnowflake == null || SpecialSnowflake != k.GetName()) {
                for (int i = 0; i < k.GetSize(); i++) {
                    Returnv = Returnv.add(k.ListOfValues.get(i).sub(newmean.GetArray().get(h).ListOfValues.get(0)));
                }
                Returnv = Returnv.div(v);
                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].ListOfValues.add(Returnv);
                Returnv = new IntValue(0);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }


    public DataFrame apply(Applyable operation) {
        SingleColumn[] Cols = new SingleColumn[Size()];
        int h = 0;
        for (SingleColumn k : GetArray()) {
            Cols[h] = new SingleColumn(k);
        }
        DataFrame returnframe = new DataFrame(Cols);
        operation.apply(returnframe);
        for (int j = 0; j < returnframe.GetArray().size(); j++) {
            if (returnframe.GetArray().get(j).GetName() == SpecialSnowflake) {
                returnframe.GetArray().set(j, new SingleColumn(this.GetArray().get(j)));

            }

        }
        return returnframe;
    }

    public void addToColumn(Value v, String id){
        for(SingleColumn k: GetArray()){
            if(k.GetName()==id){
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).add(v));
                }
                break;
            }
        }

    }

    public void subToColumn(Value v, String id){
        for(SingleColumn k: GetArray()){
            if(k.GetName()==id){
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).sub(v));
                }
                break;
            }
        }
    }

    public void mulToColumn(Value v, String id){
        for(SingleColumn k: GetArray()){
            if(k.GetName()==id){
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).mul(v));
                }
                break;
            }
        }
    }

    public void divToColumn(Value v, String id) {
        for(SingleColumn k: GetArray()){
            if(k.GetName()==id){
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).div(v));
                }
                break;
            }
        }
    }

    public  void powToColumn(Value v, String id){
        for(SingleColumn k: GetArray()){
            if(k.GetName()==id){
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).pow(v));
                }
                break;
            }
        }
    }
    public void addColToColumn(SingleColumn v, String id){

        for(SingleColumn k: GetArray()){
            if(k.GetName()==id){
                if(v.GetSize()!=k.GetSize() || v.GetType()!=k.GetType()){
                    throw new TriedToDoInvalidColOperation(k.GetName(),v.GetName());
                }
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).add(v.ListOfValues.get(i)));
                }
                break;
            }
        }

    }

    public void subColToColumn(SingleColumn v, String id){
        for(SingleColumn k: GetArray()){
            if(k.GetName()==id){
                if(v.GetSize()!=k.GetSize()|| v.GetType()!=k.GetType()){
                    throw new TriedToDoInvalidColOperation(k.GetName(),v.GetName());
                }
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).sub(v.ListOfValues.get(i)));
                }
                break;
            }
        }
    }

    public void mulColToColumn(SingleColumn v, String id){
        for(SingleColumn k: GetArray()){
            if(v.GetSize()!=k.GetSize()|| v.GetType()!=k.GetType()){
                throw new TriedToDoInvalidColOperation(k.GetName(),v.GetName());
            }
            if(k.GetName()==id){
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).mul(v.ListOfValues.get(i)));
                }
                break;
            }
        }
    }

    public void divColToColumn(SingleColumn v, String id) {
        for(SingleColumn k: GetArray()){
            if(v.GetSize()!=k.GetSize()|| v.GetType()!=k.GetType()){
                throw new TriedToDoInvalidColOperation(k.GetName(),v.GetName());
            }
            if(k.GetName()==id){
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).div(v.ListOfValues.get(i)));
                }
                break;
            }
        }
    }

    public  void powColToColumn(SingleColumn v, String id){
        for(SingleColumn k: GetArray()){
            if(v.GetSize()!=k.GetSize()|| v.GetType()!=k.GetType()){
                throw new TriedToDoInvalidColOperation(k.GetName(),v.GetName());
            }
            if(k.GetName()==id){
                for(int i=0;i<k.GetSize();i++){
                    k.ListOfValues.set(i,k.ListOfValues.get(i).pow(v.ListOfValues.get(i)));
                }
                break;
            }
        }
    }
}
