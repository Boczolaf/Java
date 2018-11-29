package dataframe.types;

import dataframe.values.FloatValue;
import dataframe.values.Value;

public class FloatType extends Type {
    @Override
    public Value fromStringToVal(String str) {
        return new FloatValue(Float.parseFloat(str));
    }
}
