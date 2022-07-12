package com.jni;

public class BLAS {
    public native double[] vxm_minPlus(int[] src,int[] dst,double[] val,int[] velIndex,double[] vecVal,int n, int m);
    public native double[] vxm_plusTimes(int[] src,int[] dst,double[] val,int[] velIndex,double[] vecVal,int n, int m);

    static
    {
        System.loadLibrary("BLAS");
    }
}
