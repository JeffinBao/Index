package com.jeffin;

import com.jeffin.util.DataTypeConvertUtil;
import com.jeffin.util.FileUtil;
import com.jeffin.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Author: baojianfeng
 * Date: 2017-11-06
 * Usage: index operations implementation,
 *        this class can create a new index file, insert a new record, find a record and list the sequential records starting from a specific key
 */
public class IndexOperation {
    private static final int INITIAL_ROOT_BLOCK_OFFSET = 1024;
    private static final int INITIAL_ALLOCATED_SPACE_OFFSET = 2048;
    private int keySize;
    private BPlusTree bPlusTree;

    /**
     * IndexOperation constructor
     * @param keySize key size
     */
    public IndexOperation(int keySize) {
        this.keySize = keySize;
        bPlusTreeInit();
    }

    /**
     * init b+ tree
     */
    private void bPlusTreeInit() {
        // calculate maximum number of tree pointers and leaf node key-pointer pairs
        int m = calMaxBranches(1024, keySize);
        int l = calMaxLeaves(1024, keySize);

        bPlusTree = new BPlusTree(m, l, keySize);
    }

    /**
     * create an index file for the source file
     */
    public void createIndexFile(String sourceFile, String desFile) {
        // create index file header
        byte[] indexFileFirstBytes = new byte[256];
        byte[] sFileBytes = sourceFile.getBytes();
        for (int i = 0; i < sFileBytes.length; i++)
            indexFileFirstBytes[i] = sFileBytes[i];
        byte[] keySizeBytes = DataTypeConvertUtil.longToBytes((long) keySize);
        byte[] initialRootAddr = DataTypeConvertUtil.longToBytes((long) INITIAL_ROOT_BLOCK_OFFSET);
        byte[] initialAllocatedSpaceOffset = DataTypeConvertUtil.longToBytes((long) INITIAL_ALLOCATED_SPACE_OFFSET);

        List<byte[]> byteArrays = new ArrayList<>();
        byteArrays.add(indexFileFirstBytes);
        byteArrays.add(keySizeBytes);
        byteArrays.add(initialRootAddr);
        byteArrays.add(initialAllocatedSpaceOffset);

        FileUtil.deleteFile(System.getProperty("user.dir") + "/" + desFile);

        // write file header into index file
        byte[] headBlock = combineByteArrays(byteArrays);
        FileUtil.writeDataIntoFile(desFile, headBlock, (long) 0);

        Map<String, Long> keyRecordMap = FileUtil.getKeyValueMap(System.getProperty("user.dir") + "/" + sourceFile, keySize); // put sourceFile into the project directory
        Set<Map.Entry<String, Long>> set = keyRecordMap.entrySet();
        Iterator<Map.Entry<String, Long>> iterator = set.iterator();
        // TODO whether need to deal with duplicate key, which will cause a fail insertion
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            bPlusTree.insert(entry.getKey(), entry.getValue(), desFile);
        }
    }

    /**
     * equation is: m * 8 + (m - 1) * keySize + 1 + 1 + 1 + 8 + 8 = blockSize
     * first 1: represents true or false, second 1: represents the current number of key, third 1: represents the current number of values
     * first 8: store the next leaf's address if exists, second 8: store parent node's address
     * @param blockSize each block size
     * @param keySize each key size
     * @return how many pointers can be in one block
     */
    private int calMaxBranches(int blockSize, int keySize) {
        return (blockSize - 19 + keySize) / (8 + keySize);
    }

    /**
     * equation is: l * (8 + keySize) + 1 + 1 + 1 + 8 + 8 = blockSize
     * how to calculate:
     * first 1: represents true or false, second 1: represents the current number of key, third 1: represents the current number of values
     * first 8: store the next leaf's address if exists, second 8: store parent node's address
     * (8 + keySize) is the length of a key-pointer pair
     * @param blockSize each block size
     * @param keySize each key size
     * @return how many leaves can be in one block
     */
    private int calMaxLeaves(int blockSize, int keySize) {
        return (blockSize - 19) / (8 + keySize);
    }

    /**
     * combine an byte array list into a bigger byte array
     * @param byteArrays byte array list
     * @return a bigger byte array
     */
    private byte[] combineByteArrays(List<byte[]> byteArrays) {
        byte[] block = new byte[1024];
        ByteBuffer buffer = ByteBuffer.wrap(block);
        for (byte[] bytes : byteArrays) {
            buffer.put(bytes);
        }

        return block;
    }


    /**
     * find a record according to the given key
     * @param key key
     * @param indexFile index file name
     * @return record related information if the record exists, otherwise return message "key was not found"
     */
    public String findRecordByKey(String key, String indexFile, String txtFile) {
        String modifiedKey = StringUtil.modifyKeyStr(key, keySize);
        long recordAddr = bPlusTree.find(modifiedKey, indexFile);
        if (recordAddr == -1L) {
            return "key not found";
        }
        int recordAddrInt = (int) recordAddr;
        String record = FileUtil.getRecordByPosition(System.getProperty("user.dir") + "/" + txtFile, recordAddr);

        StringBuilder sb = new StringBuilder();
        sb.append("At ");
        sb.append(recordAddrInt);
        sb.append(", record: ");
        sb.append(record);
        return sb.toString();
    }

    /**
     * insert a new record into txtFile and also create an index for it
     * @param key key
     * @param value value
     * @param txtFile txtFile where the record need to be inserted
     * @param indexFile indexFile where the index for the record should be created
     * @return message information about whether the insertion succeeds
     */
    public String insertNewRecord(String key, String value, String txtFile, String indexFile) {
        String insertResult = "";
        String modifiedKey = StringUtil.modifyKeyStr(key, keySize);
        long recordAddr = bPlusTree.find(modifiedKey, indexFile);
        // if key not found, insert record into txtFile and insert key-value pair into indexFile
        if (recordAddr == -1L) {
            long currentTxtFileSize = FileUtil.getFileSize(System.getProperty("user.dir") + "/" + txtFile);
            // First: add new record into txt file
            if (currentTxtFileSize != 0L) {
                String insertRecord = "\n" + key + " " + value;  // add '\n' to the record when inserting it into txt file
                byte[] recordData = insertRecord.getBytes();
                FileUtil.writeDataIntoFile(txtFile, recordData, currentTxtFileSize);
            }

            // Second: insert new key-value pair into bPlusTree
            long recordStartPosition = currentTxtFileSize + 1; // 1 is because of the position occupied by '\n'
            if (bPlusTree.insert(modifiedKey, recordStartPosition, indexFile))
                insertResult = "insert succeeded and the record position is: " + (int) recordStartPosition;
        } else {
            insertResult = "Key already exists";
        }

        return insertResult;
    }

    /**
     * list the next n records starting from the given key
     * @param key key
     * @param len next n records
     * @param indexFile index file name
     * @return records information if the key exists, otherwise print the next larger key's record and give a message indicating the key was not found
     */
    public String listSequentialRecords(String key, int len, String txtFile, String indexFile) {
        String modifiedKey = StringUtil.modifyKeyStr(key, keySize);
        // find all record positions and retrieve every record in txt file
        List<Long> addrList = bPlusTree.traverseLeafNodes(modifiedKey, len, indexFile);
        if (addrList == null)
            return "Please create index file first"; // no root node, index file doesn't exist

        StringBuilder sb = new StringBuilder();
        for (long recordPosition : addrList) {
            String record = FileUtil.getRecordByPosition(System.getProperty("user.dir") + "/" + txtFile, recordPosition);
            sb.append(record);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        while (true) {
            Scanner in = new Scanner(System.in);
            String str = in.nextLine();

            String sourceFileName = "CS6360Asg5TestData.txt";

            String[] strArray = str.split("-");
            if (strArray[0].trim().equalsIgnoreCase("index")) {
                String[] commandStr = strArray[1].split(" ", 2);
                if (commandStr[0].equalsIgnoreCase("create")) {
                    String[] varCreateArray = commandStr[1].split(" ");
                    sourceFileName = varCreateArray[0];
                    String desFileName = varCreateArray[1];
                    String keySize = varCreateArray[2];

                    IndexOperation iOper = new IndexOperation(Integer.valueOf(keySize));
                    iOper.createIndexFile(sourceFileName, desFileName);
                } else if (commandStr[0].equalsIgnoreCase("find")) {
                    String[] varFindArray = commandStr[1].split(" ");
                    String indexFileName = varFindArray[0];
                    String key = varFindArray[1];

                    int keySize = 0;
                    if (FileUtil.isFileExisted(System.getProperty("user.dir") + "/" + indexFileName))
                        keySize = (int) DataTypeConvertUtil.bytesToLong(FileUtil.getDataBlock(indexFileName, 8, 256));
                    IndexOperation iOper = new IndexOperation(keySize);
                    String findResult = iOper.findRecordByKey(key, indexFileName, sourceFileName);
                    System.out.println(findResult);
                } else if (commandStr[0].equalsIgnoreCase("insert")) {
                    String[] varInsertArray = commandStr[1].split(" ", 2);
                    String indexFileName = varInsertArray[0];
                    String record = varInsertArray[1].substring(1, varInsertArray[1].length() - 1); // remove "" characters
                    String[] keyValueArray = record.split(" ",  2); // only split the string into 2 when the first space occurs
                    String key = keyValueArray[0];
                    String value = keyValueArray[1];

                    int keySize = 0;
                    if (FileUtil.isFileExisted(System.getProperty("user.dir") + "/" + indexFileName))
                        keySize = (int) DataTypeConvertUtil.bytesToLong(FileUtil.getDataBlock(indexFileName, 8, 256));
                    IndexOperation iOper = new IndexOperation(keySize);
                    String insertResult = iOper.insertNewRecord(key, value, sourceFileName, indexFileName);
                    System.out.println(insertResult);
                } else if (commandStr[0].equalsIgnoreCase("list")) {
                    String[] varListArray = commandStr[1].split(" ");
                    String indexFileName = varListArray[0];
                    String key = varListArray[1];
                    String count = varListArray[2];

                    int keySize = 0;
                    if (FileUtil.isFileExisted(System.getProperty("user.dir") + "/" + indexFileName))
                        keySize = (int) DataTypeConvertUtil.bytesToLong(FileUtil.getDataBlock(indexFileName, 8, 256));
                    IndexOperation iOper = new IndexOperation(keySize);
                    String sequRecords = iOper.listSequentialRecords(key, Integer.valueOf(count), sourceFileName, indexFileName);
                    System.out.println(sequRecords);
                }
            } else
                System.out.println("command invalid");
        }

    }
}
