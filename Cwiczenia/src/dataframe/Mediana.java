package dataframe;

import dataframe.Interfaces.Applyable;
import dataframe.values.Value;

public class Mediana implements Applyable {
    @Override
    public DataFrame apply(DataFrame df) {
        Value Median;
        SingleColumn[] List = new SingleColumn[df.getArray().size()];
        int g = 0;
        for (SingleColumn k : df.getArray()) {
            List[g] = new SingleColumn(k.GetName(), k.GetType());
            g++;
        }
        int h = 0;
        int i = df.sizeOfCol();
        for (SingleColumn k : df.getArray()) {
            Median = k.listofvalues.get(((i - 1) / 2) - (i - 1) % 2);
            List[h].addValue(Median);
            h++;
        }
        return new DataFrame(List);
    }
}
