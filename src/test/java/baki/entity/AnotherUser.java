package baki.entity;

import com.github.chengyuxing.common.anno.Alias;

@Alias("test.user")
public class AnotherUser {
    @Alias("id")
    private Integer userId;
    @Alias("name")
    private String xm;
    @Alias("address")
    private String xxdz;
    @Alias("age")
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
