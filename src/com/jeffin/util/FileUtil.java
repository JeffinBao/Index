package com.jeffin.util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: baojianfeng
 * Date: 2017-11-06
 * Usage: Util aims to do file related operations
 */
public class FileUtil {

    /**
     * get data block from a file starting at a specific offset
     * @param fileName file
     * @param offset starting position to get the data block
     * @return byte array of the data
     */
    public static byte[] getDataBlock(String fileName, int size, long offset) {
        byte[] dataBlock = new byte[size];
        try {
            RandomAccessFile randFile = new RandomAccessFile(fileName, "rw");
            randFile.seek(offset);
            randFile.read(dataBlock);
            randFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return dataBlock;
    }

    /**
     * get a specific record by key from text file
     * @param filePath file path
     * @param offset the start position of the record
     * @return record string
     */
    public static String getRecordByPosition(String filePath, long offset) {
        String record = "";
        try {
            FileInputStream fis = new FileInputStream(filePath);
            fis.skip(offset);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            record = br.readLine();

            br.close();
            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return record;
    }

    /**
     * write byte array into a file starting at the offset
     * @param fileName file
     * @param data data byte array
     * @param offset starting position
     */
    public static void writeDataIntoFile(String fileName, byte[] data, long offset) {
        try {
            RandomAccessFile randFile = new RandomAccessFile(fileName, "rw");
            randFile.seek(offset);
            randFile.write(data);
            randFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * form a key-value map from the data in a file
     * @param filePath file
     * @param keySize key length
     * @return map containing key-value pairs
     */
    public static Map<String, Long> getKeyValueMap(String filePath, int keySize) {
        Map<String, Long> map = new HashMap<>();
        int offset = 0; // store the current offset of the first position of every line
        try {
            FileInputStream fis = new FileInputStream(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String str;
            while ((str = br.readLine()) != null) {
                String key = str.substring(0, 15);
                String modifiedKey = StringUtil.modifyKeyStr(key, keySize);

                long recordOffset = (long) offset; // key length is 15 + space is 1
                // TODO whether need to prevent duplicate keys from replacing the previous key-value in the map
                map.put(modifiedKey, recordOffset);
                offset += str.length() + 1; // 1 is for '\n' character
            }

            br.close();
            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    /**
     * get current size of a file
     * @param filePath file path
     * @return the size of a file
     */
    public static long getFileSize(String filePath) {
        long size = 0L;
        try {
            FileInputStream fis = new FileInputStream(filePath);
            size = fis.getChannel().size();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return size;
    }

    /**
     * This method is used to check whether a file exists
     * @param filePath file path
     * @return true if file exists, otherwise return false
     */
    public static boolean isFileExisted(String filePath) {
        File file = new File(filePath);

        return file.exists() && !file.isDirectory();
    }

    /**
     * delete a file if exists
     * @param filePath file path
     */
    public static void deleteFile(String filePath) {
        if (isFileExisted(filePath)) {
            File file = new File(filePath);
            file.delete();
        }
    }

    /**
     * get root address, used to find the current root's position
     * @param indexFile index file
     * @return root address
     */
    public static long getRootAddr(String indexFile) {
        byte[] rootAddrBytes = getDataBlock(indexFile, 8, 264); // file name 256 bytes, key length 8 bytes
        long rootAddr = 0L;
        if (rootAddrBytes != null)
            rootAddr = DataTypeConvertUtil.bytesToLong(rootAddrBytes);

        return rootAddr;
    }

    /**
     * get the key size of root node
     * @param indexFile index file name
     * @param rootAddr root address in index file
     * @return root key size
     */
    public static int getRootKeysSize(String indexFile, long rootAddr) {
        // the first byte is to determine whether the node is tree node, the second is to store keys size
        // so the offset is rootAddr + 1
        byte[] rootKeysSize = getDataBlock(indexFile, 1, rootAddr + 1);

        return rootKeysSize[0];
    }

    /**
     * get current space offset, used to find where to allocate a new empty block
     * @param indexFile index file
     * @return space offset
     */
    public static long getCurrentSpaceOffset(String indexFile) {
        byte[] spaceOffsetBytes = getDataBlock(indexFile, 8, 272);
        long spaceOffset = 0L;
        if (spaceOffsetBytes != null)
            spaceOffset = DataTypeConvertUtil.bytesToLong(spaceOffsetBytes);

        return spaceOffset;
    }
}
