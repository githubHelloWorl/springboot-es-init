package com.es.common;

import java.util.Random;

/**
 * author: 阿杰
 */
public class RandomNameGenerator {
    private static final String[] surnames = {
            "李", "王", "张", "刘", "陈", "杨", "赵", "黄", "周", "吴"
    };

    private static final String[] givenNames = {
            "明", "红", "芳", "伟", "静", "强", "秀英", "勇", "艳", "磊"
    };

    public static String generateRandomName() {
        Random random = new Random();
        String surname = surnames[random.nextInt(surnames.length)];
        String givenName = givenNames[random.nextInt(givenNames.length)];
        return surname + givenName;
    }

    public static int getAge(){
        Random random = new Random();
        int randomNumber = random.nextInt(90) + 10; // 生成 10 到 99 之间的随机数
        System.out.println("随机数: " + randomNumber);
        return randomNumber;
    }
    public static int getMoney(){
        Random random = new Random();
        int randomNumber = random.nextInt(90000000) + 10000000; // 生成 10,000,000 到 99,999,999 之间的随机数
        System.out.println("随机数: " + randomNumber);
        return randomNumber;
    }

    public static void main(String[] args) {
        System.out.println(generateRandomName());
    }
}
