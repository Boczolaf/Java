package dataframe;

import java.util.ArrayList;
import java.util.Objects;
//przerobic!!!!!
public class SparseDataFrame extends DataFrame {
    private ArrayList<SingleColumn> MyDataFrame = new ArrayList<SingleColumn>();
    private int OriginalLength;
    private Value Hide;
    public SparseDataFrame(){}
    public SparseDataFrame(String[] NamesOfColumns, String[] TypesOfColumns, Value Hide) {
        if (NamesOfColumns.length != TypesOfColumns.length) {
            System.out.println("Wrong input");
            return;
        }
        for (int i = 0; i < NamesOfColumns.length; ++i) {
            MyDataFrame.add(new SingleColumn(NamesOfColumns[i], TypesOfColumns[i]));
        }
        this.Hide = Hide;

    }
    public SparseDataFrame(DataFrame OtherFrame, Value Hide){
        OriginalLength = OtherFrame.Size();
        this.Hide=Hide;
        int m = 0;
        for(SingleColumn k: OtherFrame.GetArray()){
            MyDataFrame.add(new SingleColumn(k.GetName(),k.GetType()));
            for(int i = 0;i<OtherFrame.Size();i++) {
                if (k.ListOfValues.get(i) != Hide) {
                    MyDataFrame.get(m).AddCOOValue(k.ListOfValues.get(i), i);
                }
            }
                m++;
        }

    }
    @Override
    public int Size(){
        return MyDataFrame.get(0).ListOfValues.size();
    }
    public SparseDataFrame(SingleColumn[] Columns){
        for (int i=0; i<Columns.length; ++i) {
            MyDataFrame.add(Columns[i]);
            if (MyDataFrame.get(0).ListOfValues.size()!=MyDataFrame.get(i).ListOfValues.size())
                throw new RuntimeException("Wrong length of column");
        }
    }
    @Override
    public void print(){
        int Longestone = 0;
        for(SingleColumn k: MyDataFrame) {
            if(k.ListOfCoovalues.size()>Longestone){
                Longestone=k.ListOfCoovalues.size();
            }
        }





          for (int m = 0; m < MyDataFrame.size(); m++) {
              for(int i =0;i<Longestone;i++) {
                  if (i < MyDataFrame.get(m).ListOfCoovalues.size()) {
                      System.out.print(MyDataFrame.get(m).ListOfCoovalues.get(i).GetIndex());
                      System.out.print(",");
                      System.out.print(MyDataFrame.get(m).ListOfCoovalues.get(i).GetValue());
                      System.out.print("  ");
                  }


              }
              System.out.print("\n");
          }



    }
    @Override
    public void add(Value[] Values){
        if (Values.length!=MyDataFrame.size()){
            throw new RuntimeException("Invalid length of input!!!");
        }
        int iterator = 0;
        for (SingleColumn k:MyDataFrame){
            if(Values[iterator]!=Hide) {
                k.ListOfValues.add(OriginalLength,Values[iterator]);
                iterator++;
            }
            if(iterator!=0){
                OriginalLength+=1;
            }

        }
    }
    @Override
    public void add(SingleColumn Values){
        if(Values.GetSize()!=OriginalLength){
            throw new RuntimeException("Invalid length of input!!!");
        }
        SingleColumn newcolumn = new SingleColumn(Values.GetName(),Values.GetType());
        for(int i =0;i<Values.GetSize();i++){
            if(Values.ListOfValues.get(0)!=Hide){
                newcolumn.AddCOOValue(Values.ListOfValues.get(0),i);
            }
        }
        MyDataFrame.add(MyDataFrame.size(),newcolumn);

    }
@Override
    public SingleColumn get(String Colname){
        for (SingleColumn k:MyDataFrame){
            if (Objects.equals(k.GetName(),Colname)){
                return k;
            }
        }
        throw new RuntimeException("Column not found!");
    }
    @Override
    public SparseDataFrame get(String [] Cols, boolean Copy){
        SingleColumn[] Tab = new SingleColumn[Cols.length];
        for (int i=0; i<Tab.length; ++i){
            if (!Copy){
                Tab[i] = this.get(Cols[i]);
            }
            else{ //deep copy
                Tab[i] = new SingleColumn(get(Cols[i]));
            }
        }
        SparseDataFrame dataFrame = new SparseDataFrame(Tab);
        return dataFrame;

    }
    @Override
    public SparseDataFrame iloc(int from, int to){
        if(from<0 || from>=MyDataFrame.size())
            throw new IndexOutOfBoundsException("No such index: "+from);

        else if(to<0 || to>=MyDataFrame.size())
            throw new IndexOutOfBoundsException("No such index: "+to);

        else if(to<from)
            throw new IndexOutOfBoundsException("unable to create range from "+from+" to "+to);

        int size = MyDataFrame.size();
        String[] names = new String[size];
        String[] types = new String[size];
        for (int i=0; i<size; ++i){
            names[i] = MyDataFrame.get(i).GetName();
            types[i] = MyDataFrame.get(i).GetType();
        }
        SparseDataFrame NewDataFrame = new SparseDataFrame(names,types,Hide);
        Value[] tab = new Value[MyDataFrame.size()];
        for (int i=from; i<=to; ++i){
            for (int j=0; j<tab.length; j++){
                tab[j] = MyDataFrame.get(j).ListOfValues.get(i);
            }
            NewDataFrame.add(tab);

        }
        return NewDataFrame;
    }
    @Override
    public DataFrame iloc(int i){
        return iloc(i,i);

    }
    public DataFrame toDense(){
        String[] NamesOfColumns = new String[MyDataFrame.size()];
        String[] TypesOfColumns = new String[MyDataFrame.size()];
        for(int i =0;i<MyDataFrame.size();i++){
            NamesOfColumns[i]=MyDataFrame.get(i).GetName();
            TypesOfColumns[i]=MyDataFrame.get(i).GetName();
        }
        DataFrame Newdataframe = new DataFrame(NamesOfColumns,TypesOfColumns);
        Value[] List = new Value[MyDataFrame.size()];
        for(int i = 0;i<OriginalLength;i++){
            for(int k = 0; k<MyDataFrame.size();k++){
            List[k]=Hide;
            }
            Newdataframe.add(List);
        }
        int Longestone = 0;
        for(SingleColumn k: MyDataFrame) {
            if(k.ListOfCoovalues.size()>Longestone){
                Longestone=k.ListOfCoovalues.size();
            }
        }
        for(int i = 0;i<OriginalLength;i++){
            for(int k = 0; k<MyDataFrame.size();k++){
                for(int g =0;g<MyDataFrame.get(k).ListOfCoovalues.size();g++){
                    if((Integer)MyDataFrame.get(k).ListOfCoovalues.get(g).GetIndex()==i){
                        Newdataframe.GetArray().get(k).ListOfValues.set((Integer) MyDataFrame.get(k).ListOfCoovalues.get(g).GetIndex(),MyDataFrame.get(k).ListOfCoovalues.get(g));
                    }
                }
            }
        }

        return  Newdataframe;
    }
}
