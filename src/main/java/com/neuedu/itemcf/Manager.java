package com.neuedu.itemcf;

import java.util.HashMap;
import java.util.Map;

/**
 * 主控类：实现多不算法
 *
 * @author Isa
 * @date 2023-04-10
 */
public class Manager {
    private static Map<String,Integer> ACTION;

    public static Map<String,Integer> getAction() {
        return ACTION;
    }
    /**
     * private变量其他可访问
     */
    private static Map<String, String> RESOURCES;

    static {
        ACTION = new HashMap<String,Integer>();
        //click		1
        //	collect	2
        //	cart		3
        //	pay		4
        ACTION.put("click",1);
        ACTION.put("collect",2);
        ACTION.put("cart",3);
        ACTION.put("pay",4);
        //资源目录
        RESOURCES = new HashMap<String, String>();
        RESOURCES.put("step1_input","/itemcf/history");
        RESOURCES.put("step1_output","/itemcf/step1_output");
        RESOURCES.put("step2_input","/itemcf/step1_output");
        RESOURCES.put("step2_output","/itemcf/step2_output");
        RESOURCES.put("step3_input","/itemcf/step2_output");
        RESOURCES.put("step3_output","/itemcf/step3_output");
        RESOURCES.put("step4_input","/itemcf/step3_output");
        RESOURCES.put("step4_output","/itemcf/step3_output");
        RESOURCES.put("step5_input","/itemcf/step4_output");
        RESOURCES.put("step5_output","/itemcf/step5_output");
        RESOURCES.put("step6_input","/itemcf/step5_output");
        RESOURCES.put("step6_output","/itemcf/step6_output");
    }
    public static void main(String[] args){
        try {
            //串联多步算法，最终生成推荐数据
            //步骤1:数据清洗与导入
            //Step1.run(RESOURCES.get("step1_input"), RESOURCES.get("step1_output") );
            //步骤2：获取所有用户的喜欢矩阵
            //Step2.run(RESOURCES.get("step2_input"), RESOURCES.get("step2_output") );
            //步骤3：获得所有物品之间的同现矩阵
            //Step3.run(RESOURCES.get("step3_input"), RESOURCES.get("step3_output") );
            //步骤4：同现矩阵与喜欢矩阵相乘得到三维矩阵
            //Step4.run(RESOURCES.get("step4_input"), RESOURCES.get("step4_output") );
            //步骤5：三维矩阵的数据相加获得所有用户对所有物品的推荐值
            //Step5.run(RESOURCES.get("step5_input"), RESOURCES.get("step5_output") );
            //步骤6：按照推荐值降序排列
            Step6.run(RESOURCES.get("step6_input"), RESOURCES.get("step6_output") );
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
