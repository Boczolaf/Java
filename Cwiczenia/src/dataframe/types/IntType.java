package dataframe.types;

import dataframe.values.IntValue;
import dataframe.values.Value;

public class IntType extends Type {
    @Override
    public Value fromStringToVal(String str) {
        return new IntValue(Integer.parseInt(str));
    }
}
