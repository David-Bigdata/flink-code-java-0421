package com.atguigu.day07;

/**
 * Author：xiaoxin
 * Desc：java类初始化的加载顺序  先静态变量  后静态代码块    类方法  只有调用的时候才执行
 */
class test {
    public static void main(String[] args) {
        StaticClass staticClass = StaticClass.getInstance();  //调用静态类的方法
        System.out.println(StaticClass.c1);
        System.out.println(StaticClass.c2);
        System.out.println(StaticClass.c3);
    }
}

class StaticClass {
      static int c1 = 0;   //第一步 c1 = 0
      private static StaticClass staticClass = new StaticClass(); // 第二步： 初始化c1 =0 c2 =0 c3 =0   ——>执行完私有方法后  c1 =1 c2 =1 c3 =1
      static int c2 = 1;  //  第四步  c1 =1 c2 =1 c3 =1
      static int c3 = 3;  //  第五步 更改c3值 c1 =1 c2 =1 c3 =3

      static {
          c3++;  //  第六步 c1 =1 c2 =1 c3 =4
      }

      private StaticClass() {  // 第三步
          c1++;
          c2++;
          ++c3;
      }  //第三步  执行完私有方法后  c1 =1 c2 =1 c3 =1
      static StaticClass getInstance(){
          return staticClass;
      }
}


//https://www.cnblogs.com/sxkgeek/p/9647992.html