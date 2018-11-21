package dataframe;

public class Test {
    public static void main(String[] args){



        DataFrame df1 = new DataFrame("C:\\Users\\Noxianin\\IdeaProjects\\Cwiczenia\\src\\dataframe\\groupby.csv",new String[4],true);
        DataFrame dg = df1.mean();
        df1.print();
        }



    }

