package dataframe.types;

import dataframe.values.DateValue;
import dataframe.values.Value;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateType extends Type {
    @Override
    public Value fromStringToVal(String str) {
        try {
            return new DateValue(new SimpleDateFormat("dd-MM-yyyy").parse(str));
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }
    }
}
