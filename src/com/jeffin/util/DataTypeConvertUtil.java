package com.jeffin.util;

import java.nio.ByteBuffer;

/**
 * Author: baojianfeng
 * Date: 2017-11-07
 * Usage: Util aims to convert between different data types
 */
public class DataTypeConvertUtil {
    /**
     * convert long data type to byte array
     * @param n long value
     * @return byte array
     */
    public static byte[] longToBytes(long n) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(n);

        return buffer.array();
    }

    /**
     * convert byte array to long value
     * @param bytes byte array
     * @return long value
     */
    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();

        return buffer.getLong();
    }

    /**
     * convert boolean flag to byte
     * @param flag true or false value
     * @return byte
     */
    public static byte booleanToBytes(boolean flag) {
        return (byte) (flag ? 1 : 0);
    }

}
