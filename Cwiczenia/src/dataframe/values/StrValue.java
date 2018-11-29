package dataframe.values;

public class StrValue extends Value {
    private String value;

    public StrValue(String newvalue) {
        value = newvalue;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public Value add(Value v) {
        Value ReturnValue = new StrValue(this.value + v.toString());
        return ReturnValue;
    }

    @Override
    public Value sub(Value v) {
        Value ReturnValue = new StrValue(this.value);
        return ReturnValue;
    }

    @Override
    public Value mul(Value v) {
        Value ReturnValue = new StrValue(value);
        return ReturnValue;
    }

    @Override
    public Value div(Value v) {
        Value ReturnValue = new StrValue(this.value);
        return ReturnValue;
    }

    @Override
    public Value pow(Value v) {
        Value ReturnValue = new StrValue(value);
        return ReturnValue;
    }

    @Override
    public boolean eq(Value v) {
        boolean returnvalue = (value == v.toString());
        return returnvalue;
    }

    @Override
    public boolean lte(Value v) {
        boolean returnvalue = (value.length() <= v.toString().length());
        return returnvalue;
    }

    @Override
    public boolean gte(Value v) {
        boolean returnvalue = (value.length() >= v.toString().length());
        return returnvalue;
    }

    @Override
    public boolean neq(Value v) {
        boolean returnvalue = (value != v.toString());
        return returnvalue;
    }

    @Override
    public boolean equals(Object other) {
        boolean returnvalue = (value == other.toString());
        return returnvalue;
    }

    @Override
    public int hashCode() {
        int prime = 27;
        int result = 0;
        result = prime * value.hashCode() + 3000;
        return result;
    }

    @Override
    public Value create(String s) {
        Value returnvalue = new StrValue(s);
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
        return new StrValue(value);
    }
}
