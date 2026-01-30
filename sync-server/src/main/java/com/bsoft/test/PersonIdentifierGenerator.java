package com.bsoft.test;

import java.util.Random;

public class PersonIdentifierGenerator {

    private static final Random R = new Random();

    // 由你提供的医院名称提取的拼音首字母（手工预置，避免依赖额外拼音库）
    // 约定：最终的 system = <这些首字母> + "mz"
    private static final String[] HOSPITAL_INITIALS = new String[] {
            "mtzxyy","mtyy","dermyy","rayy","xyyy","jayy","hsyy","gsyy","aekkqebhyy","cdyy","tjyy","bxyy","phyy","nlyy",
            "dbyy","lzyy","xxyy","ygyy","lsyy","yfyy","akyy","sbyy","gjyy","cqyy","jlyy","xbyy","yqyy","dzyy","gnyy",
            "cbyy","wyfkyy","czyy","gyyy","cmhyy","hyyy","mtyy","tyyy","jsyy","jfyy","xyphyy","hdyy","wcyy","xyyy","jhyy",
            "cjyy","gmyy","bjyy","kcyy","cwyy","rmyy","cxyy","fyyy","dsrmyy","slyy","cnyy","rgg rmyy".replace(" ",""),
            "dsrmyy","dyyy","bayy","zyy"
    };

    /**
     * 生成一个包含“入参XML”和“回参XML”的数组：
     * [0] = 入参 XML (<PersonIdentifierQuery>)
     * [1] = 回参 XML (<PersonIdentifierQueryResponse>)
     *
     * 规则：
     * 1) 标识值保持模板格式：320682????-#######（后4位随机、后面7位随机），入参与回参一致；
     * 2) system 原“rmyymz”改为：随机医院首字母 + "mz"（入参与回参一致）。
     */
    public static String[] generateQueryAndResponse() {
        String randomValue = generateIdentifierValue();   // 320682xxxx-xxxxxxx
        String randomSystem = generateSystemCode();       // <initials> + "mz"

        // 入参 XML
        String inputXml =
                "<PersonIdentifierQuery>\n" +
                        "  <identifier nullFlavor=\"?\">\n" +
                        "    <system nullFlavor=\"\" value=\"" + randomSystem + "\"/>\n" +
                        "    <value nullFlavor=\"\" value=\"" + randomValue + "\"/>\n" +
                        "  </identifier>\n" +
                        "  <domain nullFlavor=\"\" value=\"\"/>\n" +
                        "</PersonIdentifierQuery>";

        // 回参 XML（保持与入参一致的 value；system 用随机出来的）
        String responseXml =
                "<PersonIdentifierQueryResponse>\n" +
                        "    <personIdentifer>\n" +
                        "        <system value=\"jkda\"/>\n" +
                        "        <value value=\"" + randomValue + "\"/>\n" +
                        "    </personIdentifer>\n" +
                        "    <personIdentifer>\n" +
                        "        <system value=\"" + randomSystem + "\"/>\n" +
                        "        <value value=\"" + randomValue + "\"/>\n" +
                        "    </personIdentifer>\n" +
                        "</PersonIdentifierQueryResponse>";

        return new String[] { inputXml, responseXml };
    }

    // ================== 工具方法 ==================

    // 生成“320682????-#######”：
    // - 前缀固定 "320682"
    // - 后4位：1000~9999
    // - 7位尾号：1000000~9999999
    private static String generateIdentifierValue() {
        String prefix = "320682";
        int tail4 = 1000 + R.nextInt(9000);
        int tail7 = 1000000 + R.nextInt(9000000);
        return prefix + tail4 + "-" + tail7;
    }

    // 生成 system：随机取一个医院首字母串，然后拼接 "mz"
    private static String generateSystemCode() {
        String initials = HOSPITAL_INITIALS[R.nextInt(HOSPITAL_INITIALS.length)];
        // 保证末尾不是 "mz" 的情况也兼容；统一追加 "mz"
        if (initials.endsWith("mz")) {
            return initials; // 已经带 mz
        }
        return initials + "mz";
    }

    // 示例
    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            String[] arr = generateQueryAndResponse();
            System.out.println("入参：\n" + arr[0] + "\n");
            System.out.println("回参：\n" + arr[1] + "\n----\n");
        }
    }
}
