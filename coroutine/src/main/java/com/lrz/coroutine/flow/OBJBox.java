package com.lrz.coroutine.flow;

import java.io.Serializable;

/**
 * Author:  liurongzhi
 * CreateTime:  2024/4/28
 * Description:
 */
public class OBJBox<O1, O2> implements Serializable {
    final O1 o1;
    final O2 o2;

    public OBJBox(O1 o1, O2 o2) {
        this.o1 = o1;
        this.o2 = o2;
    }
}
