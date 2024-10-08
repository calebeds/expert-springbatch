package me.calebe_oliveira.expertspringbatchapp.model;

public class SessionAction {
    public static final String SESSION_ACTION_TABLE_NAME = "session_action";

    private final long id;
    private final long userId;
    private final long actionType;
    private final double amount;

    public SessionAction(long id, long userId, long actionType, double amount) {
        this.id = id;
        this.userId = userId;
        this.actionType = actionType;
        this.amount = amount;
    }

    public long getId() {
        return id;
    }

    public long getUserId() {
        return userId;
    }

    public long getActionType() {
        return actionType;
    }

    public double getAmount() {
        return amount;
    }
}
