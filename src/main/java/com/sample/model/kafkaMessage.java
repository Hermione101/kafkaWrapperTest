package com.sample.model;


/*
This is the class object to which the message has to be serialized
 */
public class kafkaMessage {
    public String name;
    public String city;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
