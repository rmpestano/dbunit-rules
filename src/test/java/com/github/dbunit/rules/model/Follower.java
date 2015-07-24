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


    private User follower;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public User getFollower() {
        return follower;
    }

    public void setFollower(User follower) {
        this.follower = follower;
    }
}
