package dataframe.types;

import dataframe.values.COOValue;
import dataframe.values.IntValue;
import dataframe.values.Value;

public class COOType extends Type {
    @Override
    public Value fromStringToVal(String str) {
        return new COOValue(0, new IntValue(0));
    }

}
