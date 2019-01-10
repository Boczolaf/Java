package dataframe;

import dataframe.Exceptions.TriedToDoInvalidColOperation;
import dataframe.Interfaces.Applyable;
import dataframe.Interfaces.Groupby;
import dataframe.threads.*;
import dataframe.types.Type;
import dataframe.values.DblValue;
import dataframe.values.IntValue;
import dataframe.values.Value;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataFrame implements Groupby {
    private ArrayList<SingleColumn> mydataframe = new ArrayList<>();

    public DataFrame() {
    }

    private String specialSnowflake;
    private Value id;
    public Value getId(){
        return id;
    }
    public void setId(Value v){
        id=v;
    }

    public DataFrame(String[] namesofcolumns, Type[] typesofcolumns) {
        if (namesofcolumns.length != typesofcolumns.length) {
            System.out.println("Wrong input");
            return;
        }
        for (int i = 0; i < namesofcolumns.length; ++i) {
            mydataframe.add(new SingleColumn(namesofcolumns[i], typesofcolumns[i]));
        }

    }

    public int sizeOfCol() {
        return mydataframe.get(0).listofvalues.size();
    }

    public DataFrame(SingleColumn[] columns) {
        for (int i = 0; i < columns.length; ++i) {
            mydataframe.add(columns[i]);

            if (mydataframe.get(0).listofvalues.size() != mydataframe.get(i).listofvalues.size())
                throw new RuntimeException("Wrong length of column");
        }
    }
    public int width(){
        return getArray().size();
    }
    public void print() {
        for (int i = 0; i < this.sizeOfCol(); i++) {
            for (SingleColumn k : mydataframe) {
                System.out.print(k.listofvalues.get(i));
                System.out.print(" ");
            }
            System.out.print("\n");
        }
    }

    public void add(Value[] values) {
        if (values.length != mydataframe.size()) {
            throw new RuntimeException("Invalid length of input!!!");
        }
        int iterator = 0;
        for (SingleColumn k : mydataframe) {
            k.listofvalues.add(values[iterator++]);
        }
    }

    public void add(SingleColumn values) {
        if (values.GetSize() != mydataframe.get(0).GetSize()) {
            throw new RuntimeException("Invalid length of input!!!");
        }
        mydataframe.add(mydataframe.size(), values);

    }

    public SingleColumn get(String colname) {
        for (SingleColumn k : mydataframe) {
            if (Objects.equals(k.GetName(), colname)) {
                return k;
            }
        }
        throw new RuntimeException("Column not found!");
    }

    public DataFrame get(String[] cols, boolean copy) {
        SingleColumn[] Tab = new SingleColumn[cols.length];
        for (int i = 0; i < Tab.length; ++i) {
            if (!copy) {
                Tab[i] = this.get(cols[i]);
            } else { //deep copy
                Tab[i] = new SingleColumn(get(cols[i]));
            }
        }

        return new DataFrame(Tab);

    }

    public DataFrame iloc(int from, int to) {
        if (from < 0 || from >= mydataframe.size())
            throw new IndexOutOfBoundsException("No such index: " + from);

        else if (to < 0 || to >= mydataframe.size())
            throw new IndexOutOfBoundsException("No such index: " + to);

        else if (to < from)
            throw new IndexOutOfBoundsException("unable to create range from " + from + " to " + to);

        int size = mydataframe.size();
        String[] names = new String[size];
        Type[] types = new Type[size];
        for (int i = 0; i < size; ++i) {
            names[i] = mydataframe.get(i).GetName();
            types[i] = mydataframe.get(i).GetType();
        }
        DataFrame NewDataFrame = new DataFrame(names, types);
        Value[] tab = new Value[mydataframe.size()];
        for (int i = from; i <= to; ++i) {
            for (int j = 0; j < tab.length; j++) {
                tab[j] = mydataframe.get(j).listofvalues.get(i);
            }
            NewDataFrame.add(tab);

        }
        return NewDataFrame;
    }

    public ArrayList<SingleColumn> getArray() {
        return mydataframe;
    }

    public DataFrame iloc(int i) {
        return iloc(i, i);

    }
//to do

    public DataFrame(String nameoffile, String[] namesofcols, Type[] typesofcols) {
        try (BufferedReader br = new BufferedReader(new FileReader(nameoffile))) {
            String temp;

            if (namesofcols == null) {
                temp = br.readLine();
                String[] strLine = temp.split(",");
                String[] header = strLine;
                for (int i = 0; i < header.length; i++) {
                    this.mydataframe.add(new SingleColumn(strLine[i], typesofcols[i]));
                }
            } else {
                String[] header = namesofcols;
                for (int i = 0; i < header.length; i++) {
                    this.mydataframe.add(new SingleColumn(namesofcols[i], typesofcols[i]));
                }

            }
            while ((temp = br.readLine()) != null) {

                String[] strLine = temp.split(",");
                Value[] values = new Value[typesofcols.length];
                for (int i = 0; i < typesofcols.length; i++) {
                    Type currenttype = typesofcols[i];
                    values[i] = currenttype.fromStringToVal(strLine[i]);
                }
                this.add(values);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }


    }



    ArrayList<DataFrame> groupby(String id) {
        ArrayList<Value> unique = new ArrayList<>();
        ArrayList<DataFrame> returnframe = new ArrayList<>();
        Type[] types = new Type[getArray().size()-1] ;
        String[] names = new String[getArray().size()-1];
        SingleColumn specialone = get(id);
        int h=0;
        for(SingleColumn k: getArray()){
            if(k!=specialone) {
                types[h] = k.GetType();
                names[h] = k.GetName();
                h++;
            }

        }
        for(int i =0;i<sizeOfCol();i++){
            if(!unique.contains(specialone.listofvalues.get(i))){
                unique.add(specialone.listofvalues.get(i));
            }

        }
        for(int i =0;i<unique.size();i++){
            returnframe.add(new DataFrame(names,types));
            returnframe.get(i).setId(unique.get(i));
        }
        Value[] values ;
        int g ;
        for(int i =0;i<sizeOfCol();i++){

            values = new Value[getArray().size()-1];
            g=0;
            for(SingleColumn k: getArray()){
                if(k!=specialone){
                    values[g]=k.listofvalues.get(i);
                    g++;
                }
            }


            for(int j=0;j<returnframe.size();j++){
                if(specialone.listofvalues.get(i).eq(returnframe.get(j).getId())){

                    returnframe.get(j).add(values);
                    break;
                }
            }
        }
        return returnframe;
    }
    public ArrayList<DataFrame> groupbythrd(String id){
        int cores = Runtime.getRuntime().availableProcessors();
        ArrayList<Value> unique = new ArrayList<>();
        ArrayList<DataFrame> returnframe = new ArrayList<>();
        Type[] types = new Type[getArray().size()-1] ;
        String[] names = new String[getArray().size()-1];
        SingleColumn specialone = get(id);
        int h=0;
        for(SingleColumn k: getArray()){
            if(k!=specialone) {
                types[h] = k.GetType();
                names[h] = k.GetName();
                h++;
            }

        }
        for(int i =0;i<sizeOfCol();i++){
            if(!unique.contains(specialone.listofvalues.get(i))){
                unique.add(specialone.listofvalues.get(i));
            }

        }
        for(int i =0;i<unique.size();i++){
            returnframe.add(new DataFrame(names,types));
            returnframe.get(i).setId(unique.get(i));
        }


        ExecutorService executor = Executors.newFixedThreadPool(cores);
        for(int i =0;i<unique.size();i++){
            Runnable worker = new WorkerThread(this,unique.get(i),returnframe.get(i),specialone);
            executor.execute(worker);
        }
        executor.shutdown();
        try {
            while(!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                //do nothing, just wait
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdownNow();
        }
        return returnframe;
    }

    public DataFrame max() {

        Value max ;
        SingleColumn[] NewColumns = new SingleColumn[getArray().size()];
        int h = 0;
        for (SingleColumn k : getArray()) {
            if (specialSnowflake == null || specialSnowflake != k.GetName()) {
                max = getArray().get(h).listofvalues.get(0);
                for (int i = 0; i < k.GetSize(); i++) {
                    if (k.listofvalues.get(i).gte(max)) {
                        max = k.listofvalues.get(i);
                    }
                }
                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].listofvalues.add(max);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }
    public DataFrame maxthrd(){
        int cores = Runtime.getRuntime().availableProcessors();
        String[] names = new String[width()];
        Type[] types = new Type[width()];
        for(int i=0;i<width();i++){
            names[i]=getArray().get(i).GetName();
            types[i]=getArray().get(i).GetType();
        }
        DataFrame outputFrame = new DataFrame(names,types);

        ExecutorService executor = Executors.newFixedThreadPool(cores);
        for (SingleColumn k : getArray()) {
            Runnable worker = new MaxThread(k,outputFrame);
            executor.execute(worker);

        }
        executor.shutdown();
        try {
            while(!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                //do nothing, just wait
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdownNow();
        }
        return outputFrame ;
    }

    public DataFrame min() {
        Value min ;
        SingleColumn[] NewColumns = new SingleColumn[getArray().size()];
        int h = 0;
        for (SingleColumn k : getArray()) {
            if (specialSnowflake == null || specialSnowflake != k.GetName()) {
                min = getArray().get(h).listofvalues.get(0);
                for (int i = 0; i < k.GetSize(); i++) {
                    if (k.listofvalues.get(i).lte(min)) {
                        min = k.listofvalues.get(i);
                    }
                }
                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].listofvalues.add(min);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }
    public DataFrame minthrd(){
        int cores = Runtime.getRuntime().availableProcessors();
        String[] names = new String[width()];
        Type[] types = new Type[width()];
        for(int i=0;i<width();i++){
            names[i]=getArray().get(i).GetName();
            types[i]=getArray().get(i).GetType();
        }
        DataFrame outputFrame = new DataFrame(names,types);

        ExecutorService executor = Executors.newFixedThreadPool(cores);
        for (SingleColumn k : getArray()) {
            Runnable worker = new MinThread(k,outputFrame);
            executor.execute(worker);

        }
        executor.shutdown();
        try {
            while(!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                //do nothing, just wait
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdownNow();
        }
        return outputFrame ;
    }

    public DataFrame mean() {
        Value mean;
        SingleColumn[] NewColumns = new SingleColumn[getArray().size()];
        int h = 0;
        for (SingleColumn k : getArray()) {
            if (specialSnowflake == null || specialSnowflake != k.GetName()) {

                mean = getArray().get(h).listofvalues.get(0);

                for (int i = 1; i < k.GetSize(); i++) {
                    mean = mean.add(k.listofvalues.get(i));
                }

                mean = mean.div(new IntValue(k.GetSize()));

                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].listofvalues.add(mean);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }
    public DataFrame meanthrd(){
        int cores = Runtime.getRuntime().availableProcessors();
        String[] names = new String[width()];
        Type[] types = new Type[width()];
        for(int i=0;i<width();i++){
            names[i]=getArray().get(i).GetName();
            types[i]=getArray().get(i).GetType();
        }
        DataFrame outputFrame = new DataFrame(names,types);

        ExecutorService executor = Executors.newFixedThreadPool(cores);
        for (SingleColumn k : getArray()) {
            Runnable worker = new MeanThread(k,outputFrame);
            executor.execute(worker);

        }
        executor.shutdown();
        try {
            while(!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                //do nothing, just wait
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdownNow();
        }
        return outputFrame ;
    }


    public DataFrame std() {
        DataFrame newmean = this.mean();
        Value v = new IntValue(this.sizeOfCol()-1);
        SingleColumn[] NewColumns = new SingleColumn[getArray().size()];
        Value Returnv = new DblValue(0);
        int h = 0;
        for (SingleColumn k : getArray()) {
            if (specialSnowflake == null || specialSnowflake != k.GetName()) {
                for (int i = 0; i < k.GetSize(); i++) {
                    Returnv = Returnv.add((k.listofvalues.get(i).sub( newmean.getArray().get(h).listofvalues.get(0))).pow(new DblValue(2)));
                }
                Returnv = Returnv.div(v);
                Returnv = Returnv.pow(new DblValue(0.5));
                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].listofvalues.add(Returnv);
                Returnv = new DblValue(0);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }
    public DataFrame stdthrd(){
        int cores = Runtime.getRuntime().availableProcessors();
        String[] names = new String[width()];
        Type[] types = new Type[width()];
        for(int i=0;i<width();i++){
            names[i]=getArray().get(i).GetName();
            types[i]=getArray().get(i).GetType();
        }
        DataFrame outputFrame = new DataFrame(names,types);

        ExecutorService executor = Executors.newFixedThreadPool(cores);
        for (SingleColumn k : getArray()) {
            Runnable worker = new StdThread(k,outputFrame);
            executor.execute(worker);

        }
        executor.shutdown();
        try {
            while(!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                //do nothing, just wait
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdownNow();
        }
        return outputFrame ;
    }


    public DataFrame sum() {
        Value sum;
        SingleColumn[] NewColumns = new SingleColumn[getArray().size()];
        int h = 0;
        for (SingleColumn k : getArray()) {
            if (specialSnowflake == null || specialSnowflake != k.GetName()) {
                sum = getArray().get(h).listofvalues.get(0);
                for (int i = 1; i < k.GetSize(); i++) {
                    sum = sum.add(k.listofvalues.get(i));
                }
                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].listofvalues.add(sum);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }
    public DataFrame sumthrd(){
        int cores = Runtime.getRuntime().availableProcessors();
        String[] names = new String[width()];
        Type[] types = new Type[width()];
        for(int i=0;i<width();i++){
            names[i]=getArray().get(i).GetName();
            types[i]=getArray().get(i).GetType();
        }
        DataFrame outputFrame = new DataFrame(names,types);

        ExecutorService executor = Executors.newFixedThreadPool(cores);
        for (SingleColumn k : getArray()) {
            Runnable worker = new SumThread(k,outputFrame);
            executor.execute(worker);

        }
        executor.shutdown();
        try {
            while(!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                //do nothing, just wait
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdownNow();
        }
        return outputFrame ;
    }

    public DataFrame var() {
        DataFrame newmean = this.mean();
        Value v = new IntValue(this.sizeOfCol()-1);
        SingleColumn[] NewColumns = new SingleColumn[getArray().size()];
        Value Returnv = new DblValue(0);
        int h = 0;
        for (SingleColumn k : getArray()) {
            if (specialSnowflake == null || specialSnowflake != k.GetName()) {
                for (int i = 0; i < k.GetSize(); i++) {
                    Returnv = Returnv.add((k.listofvalues.get(i).sub( newmean.getArray().get(h).listofvalues.get(0))).pow(new DblValue(2)));
                }
                Returnv = Returnv.div(v);

                NewColumns[h] = new SingleColumn(k.GetName(), k.GetType());
                NewColumns[h].listofvalues.add(Returnv);
                Returnv = new DblValue(0);
                h++;
            } else {
                NewColumns[h] = new SingleColumn(k);
                h++;
            }
        }
        return new DataFrame(NewColumns);
    }
    public DataFrame varthrd(){
        int cores = Runtime.getRuntime().availableProcessors();
        String[] names = new String[width()];
        Type[] types = new Type[width()];
        for(int i=0;i<width();i++){
            names[i]=getArray().get(i).GetName();
            types[i]=getArray().get(i).GetType();
        }
        DataFrame outputFrame = new DataFrame(names,types);

        ExecutorService executor = Executors.newFixedThreadPool(cores);
        for (SingleColumn k : getArray()) {
            Runnable worker = new VarThread(k,outputFrame);
            executor.execute(worker);

        }
        executor.shutdown();
        try {
            while(!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                //do nothing, just wait
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            executor.shutdownNow();
        }
        return outputFrame ;
    }

    public DataFrame apply(Applyable operation) {
        SingleColumn[] Cols = new SingleColumn[sizeOfCol()];
        int h = 0;
        for (SingleColumn k : getArray()) {
            Cols[h] = new SingleColumn(k);
        }
        DataFrame returnframe = new DataFrame(Cols);
        operation.apply(returnframe);
        for (int j = 0; j < returnframe.getArray().size(); j++) {
            if (returnframe.getArray().get(j).GetName() == specialSnowflake) {
                returnframe.getArray().set(j, new SingleColumn(this.getArray().get(j)));

            }

        }
        return returnframe;
    }

    public void addToColumn(Value v, String id) {
        for (SingleColumn k : getArray()) {
            if (k.GetName() == id) {
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).add(v));
                }
                break;
            }
        }

    }

    public void subToColumn(Value v, String id) {
        for (SingleColumn k : getArray()) {
            if (k.GetName() == id) {
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).sub(v));
                }
                break;
            }
        }
    }

    public void mulToColumn(Value v, String id) {
        for (SingleColumn k : getArray()) {
            if (k.GetName() == id) {
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).mul(v));
                }
                break;
            }
        }
    }

    public void divToColumn(Value v, String id) {
        for (SingleColumn k : getArray()) {
            if (k.GetName() == id) {
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).div(v));
                }
                break;
            }
        }
    }

    public void powToColumn(Value v, String id) {
        for (SingleColumn k : getArray()) {
            if (k.GetName() == id) {
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).pow(v));
                }
                break;
            }
        }
    }

    public void addColToColumn(SingleColumn v, String id) {

        for (SingleColumn k : getArray()) {
            if (k.GetName() == id) {
                if (v.GetSize() != k.GetSize() || v.GetType() != k.GetType()) {
                    throw new TriedToDoInvalidColOperation(k.GetName(), v.GetName());
                }
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).add(v.listofvalues.get(i)));
                }
                break;
            }
        }

    }

    public void subColToColumn(SingleColumn v, String id) {
        for (SingleColumn k : getArray()) {
            if (k.GetName() == id) {
                if (v.GetSize() != k.GetSize() || v.GetType() != k.GetType()) {
                    throw new TriedToDoInvalidColOperation(k.GetName(), v.GetName());
                }
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).sub(v.listofvalues.get(i)));
                }
                break;
            }
        }
    }

    public void mulColToColumn(SingleColumn v, String id) {
        for (SingleColumn k : getArray()) {
            if (v.GetSize() != k.GetSize() || v.GetType() != k.GetType()) {
                throw new TriedToDoInvalidColOperation(k.GetName(), v.GetName());
            }
            if (k.GetName() == id) {
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).mul(v.listofvalues.get(i)));
                }
                break;
            }
        }
    }

    public void divColToColumn(SingleColumn v, String id) {
        for (SingleColumn k : getArray()) {
            if (v.GetSize() != k.GetSize() || v.GetType() != k.GetType()) {
                throw new TriedToDoInvalidColOperation(k.GetName(), v.GetName());
            }
            if (k.GetName() == id) {
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).div(v.listofvalues.get(i)));
                }
                break;
            }
        }
    }

    public void powColToColumn(SingleColumn v, String id) {
        for (SingleColumn k : getArray()) {
            if (v.GetSize() != k.GetSize() || v.GetType() != k.GetType()) {
                throw new TriedToDoInvalidColOperation(k.GetName(), v.GetName());
            }
            if (k.GetName() == id) {
                for (int i = 0; i < k.GetSize(); i++) {
                    k.listofvalues.set(i, k.listofvalues.get(i).pow(v.listofvalues.get(i)));
                }
                break;
            }
        }
    }
}
