package dataframe.types;

import dataframe.values.StrValue;
import dataframe.values.Value;

public class StrType extends Type {
    @Override
    public Value fromStringToVal(String str) {
        return new StrValue(str);
    }
}
