package dataframe.values;

import dataframe.Exceptions.TriedToDivideByZero;

public class IntValue extends Value {
    private int value;

    public IntValue(int newvalue) {
        value = newvalue;
    }

    @Override
    public String toString() {
        String ReturnString = Integer.toString(value);
        return ReturnString;
    }

    @Override
    public Value add(Value v) {
        Value ReturnValue = new DblValue(this.value + Double.parseDouble(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value sub(Value v) {
        Value ReturnValue = new DblValue(this.value - Double.parseDouble(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value mul(Value v) {
        Value ReturnValue = new IntValue(this.value * Integer.parseInt(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value div(Value v) {
        if (Integer.parseInt(v.toString()) == 0) {
            throw new TriedToDivideByZero("Tried to divide by zero");
        }
        Value ReturnValue = new DblValue(this.value / Double.parseDouble(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value pow(Value v) {
        Value ReturnValue = new IntValue((int) Math.pow(this.value, Integer.parseInt(v.toString())));
        return ReturnValue;
    }

    @Override
    public boolean eq(Value v) {
        boolean returnvalue = (value == Integer.parseInt(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean lte(Value v) {
        boolean returnvalue = (value <= Integer.parseInt(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean gte(Value v) {
        boolean returnvalue = (value >= Integer.parseInt(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean neq(Value v) {
        boolean returnvalue = (value != Integer.parseInt(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean equals(Object other) {
        boolean returnvalue = (value == Integer.parseInt(other.toString()));
        return returnvalue;
    }

    @Override
    public int hashCode() {
        int prime = 27;
        int result = 0;
        result = prime * value + 2000;
        return result;
    }

    @Override
    public Value create(String s) {
        Value returnvalue = new IntValue(Integer.parseInt(s));
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
        return new IntValue(value);
    }
}
