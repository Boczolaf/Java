package dataframe.values;

import java.time.LocalDateTime;

public class DateTimeValue extends Value {
    private LocalDateTime val;

    public DateTimeValue() {
    }

    public DateTimeValue(String value) {
        val = LocalDateTime.parse(value);
    }

    /**
     * Box the value
     *
     * @param value value to store
     */
    public DateTimeValue(LocalDateTime value) {
        val = value;
    }


    public LocalDateTime getValue() {
        return val;
    }

    @Override
    public String toString() {
        return val.toString();
    }

    @Override
    public DateTimeValue create(String value) {
        return new DateTimeValue(value);
    }

    @Override
    public Object getV() {
        return val;
    }


    @Override
    public DateTimeValue add(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }


    @Override
    public DateTimeValue sub(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }


    @Override
    public DateTimeValue mul(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }


    @Override
    public DateTimeValue div(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }


    @Override
    public DateTimeValue pow(Value v) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean eq(Value v) {
        if (v instanceof DateTimeValue)
            return getValue().equals(((DateTimeValue) v).getValue());
        else
            return false;
    }

    public boolean equals(Object other) {
        if (this.val == other) {
            return true;
        } else return false;
    }


    @Override
    public boolean lte(Value v) {
        if (v instanceof DateTimeValue)
            return getValue().isBefore(((DateTimeValue) v).getValue());
        else
            throw new UnsupportedOperationException();
    }


    @Override
    public boolean gte(Value v) {
        if (v instanceof DateTimeValue)
            return getValue().isAfter(((DateTimeValue) v).getValue());
        else
            throw new UnsupportedOperationException();
    }


    @Override
    public boolean neq(Value v) {
        return !eq(v);
    }

    @Override
    public int hashCode() {
        return this.val.hashCode();
    }


    protected Value clone() {
        return new DateTimeValue(val);
    }
}
