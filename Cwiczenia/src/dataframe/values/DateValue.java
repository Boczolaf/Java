package dataframe.values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateValue extends Value {
    private Date value;

    public DateValue(Date v) {
        value = v;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public Value add(Value v) {
        return new DateValue(value);
    }

    @Override
    public Value sub(Value v) {
        return new DateValue(value);
    }

    @Override
    public Value mul(Value v) {
        return new DateValue(value);
    }

    @Override
    public Value div(Value v) {
        return new DateValue(value);
    }

    @Override
    public Value pow(Value v) {
        return new DateValue(value);
    }

    @Override
    public boolean eq(Value v) {
        return value.equals(v);
    }

    @Override
    public boolean lte(Value v) {
        if (v instanceof DateValue) {
            return value.before((Date) v.getV());
        }
        return false;
    }

    @Override
    public boolean gte(Value v) {
        if (v instanceof DateValue) {
            return value.after((Date) v.getV());
        }
        return false;
    }

    @Override
    public boolean neq(Value v) {
        return !eq(v);
    }

    @Override
    public boolean equals(Object other) {
        return value.equals(other);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }


    @Override
    public Value create(String s) {
        try {
            Date date = new SimpleDateFormat("dd/MM/yyyy").parse(s);
            return new DateValue(date);
        } catch (ParseException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Object getV() {
        return value;
    }

    @Override
    protected Value clone() {
        return null;
    }
}
