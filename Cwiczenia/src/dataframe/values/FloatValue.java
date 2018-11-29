package dataframe.values;

import dataframe.Exceptions.TriedToDivideByZero;

public class FloatValue extends Value {
    private float value;

    public FloatValue(float newvalue) {
        value = newvalue;
    }

    @Override
    public String toString() {
        String ReturnString = Float.toString(value);
        return ReturnString;
    }

    @Override
    public Value add(Value v) {
        Value ReturnValue = new FloatValue(this.value + Float.parseFloat(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value sub(Value v) {
        Value ReturnValue = new FloatValue(this.value - Float.parseFloat(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value mul(Value v) {
        Value ReturnValue = new FloatValue(this.value * Float.parseFloat(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value div(Value v) {
        if (Float.parseFloat(v.toString()) == 0) {
            throw new TriedToDivideByZero("Tried to divide by zero");
        }
        Value ReturnValue = new DblValue(this.value / Double.parseDouble(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value pow(Value v) {
        Value ReturnValue = new FloatValue((int) Math.pow(this.value, Float.parseFloat(v.toString())));
        return ReturnValue;
    }

    @Override
    public boolean eq(Value v) {
        boolean returnvalue = (value == Float.parseFloat(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean lte(Value v) {
        boolean returnvalue = (value <= Float.parseFloat(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean gte(Value v) {
        boolean returnvalue = (value >= Float.parseFloat(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean neq(Value v) {
        boolean returnvalue = (value != Float.parseFloat(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean equals(Object other) {
        boolean returnvalue = (value == Float.parseFloat(other.toString()));
        return returnvalue;
    }

    @Override
    public int hashCode() {
        int prime = 27;
        int result = 0;
        result = prime * (int) value + 20000;
        return result;
    }

    @Override
    public Value create(String s) {
        Value returnvalue = new FloatValue(Float.parseFloat(s));
        return returnvalue;
    }

    @Override
    public Object getV() {
        return value;
    }

    public void print() {
        System.out.print(this.value);
        System.out.print("\n");
    }


    protected Value clone() {
        return new FloatValue(value);
    }
}
