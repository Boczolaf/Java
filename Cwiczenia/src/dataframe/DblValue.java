package dataframe;

public class DblValue extends Value{
    public double value;
    DblValue(double newvalue){
        value = newvalue;
    }
    @Override
    public String toString() {
        String ReturnString = Double.toString(value);
        return ReturnString;
    }

    @Override
    public Value add(Value v) {
        Value ReturnValue = new DblValue(this.value+Double.parseDouble(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value sub(Value v) {
        Value ReturnValue = new DblValue(this.value-Double.parseDouble(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value mul(Value v) {
        Value ReturnValue = new DblValue(this.value*Double.parseDouble(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value div(Value v)   {
        if(Double.parseDouble(v.toString())==0){
            throw new TriedToDivideByZero("Tried to divide by zero");
        }
        Value ReturnValue = new DblValue(this.value/Double.parseDouble(v.toString()));
        return ReturnValue;
    }

    @Override
    public Value pow(Value v) {
        Value ReturnValue = new DblValue((int)Math.pow(this.value,Double.parseDouble(v.toString())));
        return ReturnValue;
    }

    @Override
    public boolean eq(Value v) {
        boolean returnvalue = (value==Double.parseDouble(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean lte(Value v) {
        boolean returnvalue = (value<=Double.parseDouble(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean gte(Value v) {
        boolean returnvalue = (value>=Double.parseDouble(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean neq(Value v) {
        boolean returnvalue = (value!=Double.parseDouble(v.toString()));
        return returnvalue;
    }

    @Override
    public boolean equals(Object other) {
        boolean returnvalue = (value==Double.parseDouble(other.toString()));
        return returnvalue;
    }

    @Override
    public int hashCode() {
        int prime = 27;
        int result=0;
        result = prime*(int)value+7000;
        return result;
    }

    @Override
    public Value create(String s) {
        Value returnvalue = new DblValue(Double.parseDouble(s));
        return returnvalue;
    }
    public void print(){
        System.out.print(this.value);
        System.out.print("\n");
    }
    @Override
    public Object GetValue() {
        return this.value;
    }

    protected Value clone(){
        return new DblValue(value);
    }
}
