package dataframe;
import java.util.ArrayList;

public class COOValue extends Value{
    int Index;
    Value value;
    public COOValue(int NewIndex, Value NewValue){
        int k = (Integer) NewIndex;
       Index=k;
        value = NewValue;
    }
    public Object GetIndex(){
        return Index;
    }
    @Override
    public Object GetValue(){
        return value;
    }
    @Override
    public String toString() {
        String ReturnString = value.toString();
        return ReturnString;
    }

    @Override
    public Value add(Value v) {
        Value ReturnValue = new COOValue(Index,v.add(value));
        return ReturnValue;
    }

    @Override
    public Value sub(Value v) {
        Value ReturnValue = new COOValue(Index,value.sub(v));
        return ReturnValue;
    }

    @Override
    public Value mul(Value v) {
        Value ReturnValue = new COOValue(Index,value.mul(v));
        return ReturnValue;
    }

    @Override
    public Value div(Value v)   {
        Value ReturnValue = new COOValue(Index, value.div(v));
        return ReturnValue;
    }

    @Override
    public Value pow(Value v) {
        Value ReturnValue = new COOValue(Index,value.pow(v));
        return ReturnValue;
    }

    @Override
    public boolean eq(Value v) {
        boolean returnvalue = (value.toString()==v.toString());
        return returnvalue;
    }

    @Override
    public boolean lte(Value v) {
        boolean returnvalue = (value.lte(v));
        return returnvalue;
    }

    @Override
    public boolean gte(Value v) {
        boolean returnvalue = (value.gte(v));
        return returnvalue;
    }

    @Override
    public boolean neq(Value v) {
        boolean returnvalue = (value.neq(v));
        return returnvalue;
    }

    @Override
    public boolean equals(Object other) {
        boolean returnvalue = (value.equals(other));
        return returnvalue;
    }

    @Override
    public int hashCode() {
        int prime = 27;
        int result=0;
        result = prime*value.hashCode()+20000;
        return result;
    }

    @Override
    public Value create(String s) {
        Value returnvalue = new COOValue(Index,new StrValue(s));
        return returnvalue;
    }
    public void print(){
        System.out.print(this.value);
        System.out.print("\n");
    }

    protected Value clone(){
        return new COOValue(Index,value);
    }
}
