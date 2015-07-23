package com.github.dbunit.rules.model;

import javax.persistence.*;

/**
 * Created by pestano on 22/07/15.
 */
@Entity
public class Follower {

    @Id
    @GeneratedValue
    private long id;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private User user;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }
}
