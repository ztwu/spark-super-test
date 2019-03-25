package com.iflytek.edcc;

import java.io.Serializable;

/**
 * created with idea
 * user:ztwu
 * date:2019/3/25
 * description
 */
public class Event2 implements Serializable {

    private String actoruserId;
    private String actionId;
    private int actionNum;

    public String getActoruserId() {
        return actoruserId;
    }

    public void setActoruserId(String actoruserId) {
        this.actoruserId = actoruserId;
    }

    public String getActionId() {
        return actionId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public int getActionNum() {
        return actionNum;
    }

    public void setActionNum(int actionNum) {
        this.actionNum = actionNum;
    }

    public Event2() {

    }

    public Event2(String actoruserId, String actionId, int actionNum) {
        this.actoruserId = actoruserId;
        this.actionId = actionId;
        this.actionNum = actionNum;
    }
}
