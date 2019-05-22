package com.gaeainfo.entity;

import com.sun.org.apache.regexp.internal.RE;

public class Response {
    private ResultCode a;
    private String b;
    private String c;

    public Response(ResultCode a, String b, String c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public ResultCode getA() {
        return a;
    }

    public void setA(ResultCode a) {
        this.a = a;
    }

    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

    public String getC() {
        return c;
    }

    public void setC(String c) {
        this.c = c;
    }
}
