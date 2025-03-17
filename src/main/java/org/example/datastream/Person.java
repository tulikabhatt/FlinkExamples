package org.example.datastream;

import java.io.Serializable;

public class Person {
    private int age;
    private String name;

    // Constructor
    public Person(int age, String name) {
        this.age = age;
        this.name = name;
    }

    // Getters
    public int getAge() { return age; }
    public String getName() { return name; }

    @Override
    public String toString() {
        return "Person{age=" + age + ", name='" + name + "'}";
    }
}