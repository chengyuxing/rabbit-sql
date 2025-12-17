package baki.entity;

import baki.entityExecutor.LazyReference;

import javax.persistence.*;

@Entity
@Table(schema = "test", name = "guest")
public class Guest {
    @Id
    private Integer id;
    @Column(name = "name")
    private String xm;
    private Integer age;
    // FIXME 懒加载的情况下，要考虑下，如何为关联对象构建实体查询提供 字符串 where 条件，现在的方法引用好像做不到User::getId
    private LazyReference<User> address;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getXm() {
        return xm;
    }

    public void setXm(String xm) {
        this.xm = xm;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public LazyReference<User> getAddress() {
        return address;
    }

    public void setAddress(LazyReference<User> address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "Guest{" +
                "id=" + id +
                ", xm='" + xm + '\'' +
                ", age=" + age +
                ", address='" + address + '\'' +
                '}';
    }
}
