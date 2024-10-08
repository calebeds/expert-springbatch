package me.calebe_oliveira.expertspringbatchapp.model;

public class UserScoreUpdate {
    public static final String USER_SCORE_TABLE_NAME = "user_score";

    private final long userId;
    private final double add;
    private final double multiply;

    public UserScoreUpdate(long userId, double add, double multiply) {
        this.userId = userId;
        this.add = add;
        this.multiply = multiply;
    }

    public long getUserId() {
        return userId;
    }

    public double getAdd() {
        return add;
    }

    public double getMultiply() {
        return multiply;
    }
}
