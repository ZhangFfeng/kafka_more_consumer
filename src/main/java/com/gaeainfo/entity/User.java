package com.gaeainfo.entity;

public class User {
    private Response response;
    private int age;
    private String name;

    public User() {
    }

    public Response getId() {
        return response;
    }

    public void setId(Response response) {
        this.response = response;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
