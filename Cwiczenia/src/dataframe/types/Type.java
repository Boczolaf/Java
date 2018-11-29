package dataframe.types;

import dataframe.values.Value;

public abstract class Type {
    public abstract Value fromStringToVal(String str);
}
