package com.bsoft.common.util;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

/**
 * ClassName: JobIdUtil
 * Package:   com.bsoft.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/13 - 14:44
 */
public class JobIdUtil {
    public static String generateFixedJobId(String clientId) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(clientId.getBytes(StandardCharsets.UTF_8));
            BigInteger bigInt = new BigInteger(1, hash);
            return String.format("%032x", bigInt);
        } catch (Exception e) {
            throw new RuntimeException("生成 JobID 失败", e);
        }
    }



    /**
     * 生成随机 Job ID
     * @return 32位不带短横线的小写字母 job id
     */
    public static String generateRandomJobId() {
        return UUID.randomUUID().toString().replace("-", "").toLowerCase();
    }
}

