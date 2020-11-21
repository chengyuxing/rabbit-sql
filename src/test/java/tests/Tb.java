package tests;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Tb {
    private int a;
    private int b;
    private List<String> strs;
    private List<Integer> ints;
    private Me jsb;
    private List<Map<String, Object>> js;
    private String str;
    private Date ts;
    private Date dt;
    private byte[] blob;

    @Override
    public String toString() {
        return "Tb{" +
                "a=" + a +
                ", b=" + b +
                ", strs=" + strs +
                ", ints=" + ints +
                ", jsb=" + jsb +
                ", js=" + js +
                ", str='" + str + '\'' +
                ", ts=" + ts +
                ", dt=" + dt +
                ", blob=" + blob +
                '}';
    }

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public int getB() {
        return b;
    }

    public void setB(int b) {
        this.b = b;
    }

    public List<String> getStrs() {
        return strs;
    }

    public void setStrs(List<String> strs) {
        this.strs = strs;
    }

    public List<Integer> getInts() {
        return ints;
    }

    public void setInts(List<Integer> ints) {
        this.ints = ints;
    }

    public Me getJsb() {
        return jsb;
    }

    public void setJsb(Me jsb) {
        this.jsb = jsb;
    }

    public List<Map<String, Object>> getJs() {
        return js;
    }

    public void setJs(List<Map<String, Object>> js) {
        this.js = js;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public Date getTs() {
        return ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }

    public Date getDt() {
        return dt;
    }

    public void setDt(Date dt) {
        this.dt = dt;
    }

    public byte[] getBlob() {
        return blob;
    }

    public void setBlob(byte[] blob) {
        this.blob = blob;
    }
}
