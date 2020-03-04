package com.qinghua.mapreduce.join的应用.reduceJoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String pid;
    private String id;
    private int amount;
    private String pname;

    @Override
    public String toString() {
        return "OrderBean{" +
                "pid='" + pid + '\'' +
                ", id='" + id + '\'' +
                ", amount=" + amount +
                ", pname='" + pname + '\'' +
                '}';
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public int compareTo(OrderBean o) {
        int compare = this.pid.compareTo(o.getPid());
        if (compare == 0) {
            int i = o.getPname().compareTo(this.pname);
            return i;
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pid);
        out.writeUTF(id);
        out.writeInt(amount);
        out.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pid = in.readUTF();
        id = in.readUTF();
        amount = in.readInt();
        pname = in.readUTF();
    }
}
