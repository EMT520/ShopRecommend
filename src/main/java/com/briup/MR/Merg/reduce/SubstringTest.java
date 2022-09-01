package com.briup.MR.Merg.reduce;

public class SubstringTest {
    public static void main(String[] args) {
        String str="00001,Moon Light,1973";
        String k=str.substring(0,str.indexOf(","));
        String v=str.substring(str.indexOf(",")+1,str.length());
        String b= str.substring(1);
        System.out.println(k);
        System.out.println(v);
        System.out.println(b);
    }
}
