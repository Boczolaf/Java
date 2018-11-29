package dataframe.Exceptions;

public class TriedToDivideByZero extends RuntimeException {
    public TriedToDivideByZero(String message) {
        System.out.print(message);
    }
}
