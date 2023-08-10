package baki.entity;

import baki.entityExecutor.annotation.Col;
import baki.entityExecutor.annotation.Table;

import java.time.LocalDateTime;

@Table(schema = "test", value = "user")
public class User {
    @Col
    private Integer id;
    @Col
    private String name;
    @Col
    private Integer age;
    @Col
    private String address;
    @Col("create_time")
    private LocalDateTime dt;

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", address='" + address + '\'' +
                ", dt=" + dt +
                '}';
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public LocalDateTime getDt() {
        return dt;
    }

    public void setDt(LocalDateTime dt) {
        this.dt = dt;
    }
}
