package dataframe;

public class Mediana implements Applyable {
    @Override
    public DataFrame apply(DataFrame df) {
        Value Median ;
        SingleColumn[] List = new SingleColumn[df.GetArray().size()];
        int g=0;
        for(SingleColumn k: df.GetArray()){
            List[g]= new SingleColumn(k.GetName(),k.GetType());
            g++;
        }
        int h =0;
        int i = df.Size();
        for(SingleColumn k: df.GetArray()){
            Median=k.ListOfValues.get(((i-1)/2)-(i-1)%2);
            List[h].addValue(Median);
            h++;
        }
        return new DataFrame(List);
    }
}
