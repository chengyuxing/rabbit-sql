package baki.entity;

import javax.persistence.*;

@Entity
@Table(schema = "test", name = "guest")
public class Guest {
    @Id
    private Integer id;
    @Column(name = "name")
    private String xm;
    private Integer age;
    private String address;

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

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
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
