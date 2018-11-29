package dataframe;

import dataframe.types.Type;
import dataframe.values.Value;

import java.util.ArrayList;
import java.util.Objects;

//przerobic!!!!!
public class SparseDataFrame extends DataFrame {
    private ArrayList<SingleColumn> MyDataFrame = new ArrayList<SingleColumn>();
    private int OriginalLength;
    private Value Hide;

    public SparseDataFrame() {
    }

    public SparseDataFrame(String[] NamesOfColumns, Type[] TypesOfColumns, Value Hide) {
        if (NamesOfColumns.length != TypesOfColumns.length) {
            System.out.println("Wrong input");
            return;
        }
        for (int i = 0; i < NamesOfColumns.length; ++i) {
            MyDataFrame.add(new SingleColumn(NamesOfColumns[i], TypesOfColumns[i]));
        }
        this.Hide = Hide;

    }

    public SparseDataFrame(DataFrame OtherFrame, Value Hide) {
        OriginalLength = OtherFrame.sizeOfCol();
        this.Hide = Hide;
        int m = 0;
        for (SingleColumn k : OtherFrame.getArray()) {
            MyDataFrame.add(new SingleColumn(k.GetName(), k.GetType()));
            for (int i = 0; i < OtherFrame.sizeOfCol(); i++) {
                if (k.listofvalues.get(i) != Hide) {
                    MyDataFrame.get(m).AddCOOValue(k.listofvalues.get(i), i);
                }
            }
            m++;
        }

    }

    @Override
    public int sizeOfCol() {
        return MyDataFrame.get(0).listofvalues.size();
    }

    public SparseDataFrame(SingleColumn[] Columns) {
        for (int i = 0; i < Columns.length; ++i) {
            MyDataFrame.add(Columns[i]);
            if (MyDataFrame.get(0).listofvalues.size() != MyDataFrame.get(i).listofvalues.size())
                throw new RuntimeException("Wrong length of column");
        }
    }

    @Override
    public void print() {
        int Longestone = 0;
        for (SingleColumn k : MyDataFrame) {
            if (k.listofcoovalues.size() > Longestone) {
                Longestone = k.listofcoovalues.size();
            }
        }


        for (int m = 0; m < MyDataFrame.size(); m++) {
            for (int i = 0; i < Longestone; i++) {
                if (i < MyDataFrame.get(m).listofcoovalues.size()) {
                    System.out.print(MyDataFrame.get(m).listofcoovalues.get(i).GetIndex());
                    System.out.print(",");
                    System.out.print(MyDataFrame.get(m).listofcoovalues.get(i).getV());
                    System.out.print("  ");
                }


            }
            System.out.print("\n");
        }


    }

    @Override
    public void add(Value[] Values) {
        if (Values.length != MyDataFrame.size()) {
            throw new RuntimeException("Invalid length of input!!!");
        }
        int iterator = 0;
        for (SingleColumn k : MyDataFrame) {
            if (Values[iterator] != Hide) {
                k.listofvalues.add(OriginalLength, Values[iterator]);
                iterator++;
            }
            if (iterator != 0) {
                OriginalLength += 1;
            }

        }
    }

    @Override
    public void add(SingleColumn values) {
        if (values.GetSize() != OriginalLength) {
            throw new RuntimeException("Invalid length of input!!!");
        }
        SingleColumn newcolumn = new SingleColumn(values.GetName(), values.GetType());
        for (int i = 0; i < values.GetSize(); i++) {
            if (values.listofvalues.get(0) != Hide) {
                newcolumn.AddCOOValue(values.listofvalues.get(0), i);
            }
        }
        MyDataFrame.add(MyDataFrame.size(), newcolumn);

    }

    @Override
    public SingleColumn get(String Colname) {
        for (SingleColumn k : MyDataFrame) {
            if (Objects.equals(k.GetName(), Colname)) {
                return k;
            }
        }
        throw new RuntimeException("Column not found!");
    }

    @Override
    public SparseDataFrame get(String[] Cols, boolean Copy) {
        SingleColumn[] Tab = new SingleColumn[Cols.length];
        for (int i = 0; i < Tab.length; ++i) {
            if (!Copy) {
                Tab[i] = this.get(Cols[i]);
            } else { //deep copy
                Tab[i] = new SingleColumn(get(Cols[i]));
            }
        }
        SparseDataFrame dataFrame = new SparseDataFrame(Tab);
        return dataFrame;

    }

    @Override
    public SparseDataFrame iloc(int from, int to) {
        if (from < 0 || from >= MyDataFrame.size())
            throw new IndexOutOfBoundsException("No such index: " + from);

        else if (to < 0 || to >= MyDataFrame.size())
            throw new IndexOutOfBoundsException("No such index: " + to);

        else if (to < from)
            throw new IndexOutOfBoundsException("unable to create range from " + from + " to " + to);

        int size = MyDataFrame.size();
        String[] names = new String[size];
        Type[] types = new Type[size];
        for (int i = 0; i < size; ++i) {
            names[i] = MyDataFrame.get(i).GetName();
            types[i] = MyDataFrame.get(i).GetType();
        }
        SparseDataFrame NewDataFrame = new SparseDataFrame(names, types, Hide);
        Value[] tab = new Value[MyDataFrame.size()];
        for (int i = from; i <= to; ++i) {
            for (int j = 0; j < tab.length; j++) {
                tab[j] = MyDataFrame.get(j).listofvalues.get(i);
            }
            NewDataFrame.add(tab);

        }
        return NewDataFrame;
    }

    @Override
    public DataFrame iloc(int i) {
        return iloc(i, i);

    }

    public DataFrame toDense() {
        String[] NamesOfColumns = new String[MyDataFrame.size()];
        Type[] TypesOfColumns = new Type[MyDataFrame.size()];
        for (int i = 0; i < MyDataFrame.size(); i++) {
            NamesOfColumns[i] = MyDataFrame.get(i).GetName();
            TypesOfColumns[i] = MyDataFrame.get(i).GetType();
        }
        DataFrame Newdataframe = new DataFrame(NamesOfColumns, TypesOfColumns);
        Value[] List = new Value[MyDataFrame.size()];
        for (int i = 0; i < OriginalLength; i++) {
            for (int k = 0; k < MyDataFrame.size(); k++) {
                List[k] = Hide;
            }
            Newdataframe.add(List);
        }
        int Longestone = 0;
        for (SingleColumn k : MyDataFrame) {
            if (k.listofcoovalues.size() > Longestone) {
                Longestone = k.listofcoovalues.size();
            }
        }
        for (int i = 0; i < OriginalLength; i++) {
            for (int k = 0; k < MyDataFrame.size(); k++) {
                for (int g = 0; g < MyDataFrame.get(k).listofcoovalues.size(); g++) {
                    if ((Integer) MyDataFrame.get(k).listofcoovalues.get(g).GetIndex() == i) {
                        Newdataframe.getArray().get(k).listofvalues.set((Integer) MyDataFrame.get(k).listofcoovalues.get(g).GetIndex(), MyDataFrame.get(k).listofcoovalues.get(g));
                    }
                }
            }
        }

        return Newdataframe;
    }
}
