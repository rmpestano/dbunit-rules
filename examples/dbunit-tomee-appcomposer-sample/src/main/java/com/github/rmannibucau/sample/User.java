package com.github.rmannibucau.sample;


import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class User {
    @Id
    private long id;

    private String name;

    public String getName() {
        return name;
    }
}
