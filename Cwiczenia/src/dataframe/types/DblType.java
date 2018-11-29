package dataframe.types;

import dataframe.values.DblValue;
import dataframe.values.Value;

public class DblType extends Type {
    @Override
    public Value fromStringToVal(String str) {
        return new DblValue(Double.parseDouble(str));
    }
}
