package baki.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(schema = "test")
public class AnotherUser {
    @Id
    private Integer userId;
    @Column(name = "name")
    private String xm;
    @Column(name = "address")
    private String xxdz;
    @Column(name = "age")
    private Integer nl;

    @Override
    public String toString() {
        return "AnotherUser{" +
                "xm='" + xm + '\'' +
                ", xxdz='" + xxdz + '\'' +
                ", nl='" + nl + '\'' +
                '}';
    }

    public String getXm() {
        return xm;
    }

    public void setXm(String xm) {
        this.xm = xm;
    }

    public String getXxdz() {
        return xxdz;
    }

    public void setXxdz(String xxdz) {
        this.xxdz = xxdz;
    }

    public Integer getNl() {
        return nl;
    }

    public void setNl(Integer nl) {
        this.nl = nl;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }
}
