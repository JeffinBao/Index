package com.jeffin.util;

/**
 * Author: baojianfeng
 * Date: 2017-11-14
 */
public class StringUtil {

    /**
     * modify key string according to the given key size
     * @param originalKey original key string
     * @param keySize given key size
     * @return modified key string
     */
    public static String modifyKeyStr(String originalKey, int keySize) {
        String modifiedKey = originalKey;
        // if key length is greater than keySize, truncate it
        // if key length is less tan keySize, pad it with blank
        if (keySize > originalKey.length()) {
            for (int i = 0; i < keySize - originalKey.length(); i++) {
                modifiedKey = modifiedKey.concat(" ");
            }
        } else if (keySize < originalKey.length()) {
            modifiedKey = modifiedKey.substring(0, keySize);
        }

        return modifiedKey;
    }
}
