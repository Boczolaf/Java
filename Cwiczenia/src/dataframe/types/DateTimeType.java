package dataframe.types;

import dataframe.values.DateTimeValue;
import dataframe.values.Value;

public class DateTimeType extends Type {
    @Override
    public Value fromStringToVal(String str) {
        return new DateTimeValue(str);
    }
}
