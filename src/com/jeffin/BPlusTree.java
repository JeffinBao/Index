package com.jeffin;

import com.jeffin.util.DataTypeConvertUtil;
import com.jeffin.util.FileUtil;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Author: baojianfeng
 * Date: 2017-11-05
 * Usage: A b+ tree has insert, find, traverseLeafNode functions
 */
public class BPlusTree {
    private Node root;
    private int m;          // m is the maximum tree pointer size
    private int l;          // l is the maximum leaf node key/value pair size
    private int keySize;

    /**
     * B+ tree constructor
     * @param m maximum number of child nodes allowed in tree node
     * @param l maximum number of key-value pair allowed in leaf node
     * @param keySize key size
     */
    public BPlusTree(int m, int l, int keySize) {
        this.m = m;
        this.l = l;
        this.keySize = keySize;
    }

    /**
     * insert a key/value pair, if the key already exists, don't insert the they
     * @param key key
     * @param value value
     * @return true if insert success, false if the key already exists
     */
    public boolean insert(String key, long value, String indexFile) {
        if (root == null) {
            if (FileUtil.isFileExisted(System.getProperty("user.dir") + "/" + indexFile))
                setRoot(indexFile);
        }

        // there are no elements in root, means it just begins inserting
        if (root.keys.size() == 0) {
            root = new LeafNode();
            root.setStartPos(1024); // when inserting the first key-value pair into index file, root start position is the next block after header block
        }

        // update root every time inserting new element
        if (root.keys.size() < FileUtil.getRootKeysSize(indexFile, FileUtil.getRootAddr(indexFile)))
            setRoot(indexFile);
        LeafNode lf = (LeafNode) findLeaf(root, key, indexFile);
        boolean insertSuc = lf.insert(key, value);
        if (!insertSuc)
            return false;

        if (lf.getKeysSize() > l) {
            splitAndRebalance(lf, indexFile);
        } else {
            writeNodeIntoIndexFile(lf, indexFile);
        }

        return true;
    }

    /**
     * split and rebalance the b+ tree
     * @param node node
     * @param indexFile index file
     */
    private void splitAndRebalance(Node node, String indexFile) {
        if (node.isLeafNode) {
            LeafNode lf = (LeafNode) node;
            List<String> leftKeys = lf.keys.subList(0, lf.keys.size() / 2);
            List<String> rightKeys = lf.keys.subList(lf.keys.size() / 2, lf.keys.size());
            List<Long> leftValues = lf.values.subList(0, lf.values.size() / 2);
            List<Long> rightValues = lf.values.subList(lf.values.size() / 2, lf.values.size());

            long curSpaceOffset = FileUtil.getCurrentSpaceOffset(indexFile);
            if (lf.nextLeaf == null)
                lf.nextLeaf = 0L;
            LeafNode lfRight = new LeafNode(rightKeys, rightValues, lf.nextLeaf);
            lfRight.setStartPos(curSpaceOffset);
            curSpaceOffset += 1024; // be ready for allocating the next block
            LeafNode lfLeft = new LeafNode(leftKeys, leftValues, lfRight.getStartPos());
            lfLeft.setStartPos(lf.getStartPos());

            String key = lf.keys.get(lf.keys.size() / 2);
            if (lf.getParentNodeAddr() == 0L) { // TODO need to test whether it's 0L or null
                InternalNode in = new InternalNode(key, lfLeft.getStartPos(), lfRight.getStartPos());
                in.setStartPos(curSpaceOffset);
                curSpaceOffset += 1024;
                root = in;
                lfLeft.setParentNodeAddr(root.getStartPos());
                lfRight.setParentNodeAddr(root.getStartPos());

                // write two leaf node and root node into index file
                writeNodeIntoIndexFile(lfLeft, indexFile);
                writeNodeIntoIndexFile(lfRight, indexFile);
                writeNodeIntoIndexFile(root, indexFile);

                // update root start position and current space offset
                FileUtil.writeDataIntoFile(indexFile, DataTypeConvertUtil.longToBytes(root.getStartPos()), 264);
                FileUtil.writeDataIntoFile(indexFile, DataTypeConvertUtil.longToBytes(curSpaceOffset), 272);

            } else {
                InternalNode in = (InternalNode) retrieveNodeFromDisk(indexFile, lf.getParentNodeAddr());
                in.insert(key, lfLeft.getStartPos(), lfRight.getStartPos()); // insert the key from child node into parent node, also add left child and right child pointers

                lfLeft.setParentNodeAddr(in.getStartPos());
                lfRight.setParentNodeAddr(in.getStartPos());

                if (in.getValueSize() > m) {
                    splitAndRebalance(in, indexFile);
                } else {
                    // write two leaf nodes and parent node into index file
                    writeNodeIntoIndexFile(lfLeft, indexFile);
                    writeNodeIntoIndexFile(lfRight, indexFile);
                    writeNodeIntoIndexFile(in, indexFile);

                    // update current space offset
                    FileUtil.writeDataIntoFile(indexFile, DataTypeConvertUtil.longToBytes(curSpaceOffset), 272);
                }
            }
        } else {
            InternalNode in = (InternalNode) node;
            List<String> leftKeys = null;
            if (in.keys.size() / 2 - 1 > 0)  // if keys only has 3 elements, then subList(0, 0) will return empty list, split fails
                leftKeys = in.keys.subList(0, in.keys.size() / 2);
            else
                leftKeys = in.keys.subList(0, 1);

            List<String> rightKeys = in.keys.subList(in.keys.size() / 2 + 1, in.keys.size());
            List<Long> leftValues = in.values.subList(0, in.values.size() / 2);
            List<Long> rightValues = in.values.subList(in.values.size() / 2, in.values.size());

            long curSpaceOffset = FileUtil.getCurrentSpaceOffset(indexFile);
            InternalNode inLeft = new InternalNode(leftKeys, leftValues);
            inLeft.setStartPos(in.getStartPos());
            InternalNode inRight = new InternalNode(rightKeys, rightValues);
            inRight.setStartPos(curSpaceOffset);
            curSpaceOffset += 1024;

            String key = in.keys.get(in.keys.size() / 2);
            // to check whether a node has parent node,
            // if it doesn't have a parent node, split and create a parent node which is the root node
            // if it has a parent node, split and insert the middle key to the parent node
            if (in.getParentNodeAddr() == 0L) {
                InternalNode inParent = new InternalNode(key, inLeft.getStartPos(), inRight.getStartPos());
                inParent.setStartPos(curSpaceOffset);
                curSpaceOffset += 1024;
                root = inParent;

                inLeft.setParentNodeAddr(root.getStartPos());
                inRight.setParentNodeAddr(root.getStartPos());

                // write two leaf node and root node into file
                writeNodeIntoIndexFile(inLeft, indexFile);
                writeNodeIntoIndexFile(inRight, indexFile);
                writeNodeIntoIndexFile(root, indexFile);

                // update root start position and current space offset
                FileUtil.writeDataIntoFile(indexFile, DataTypeConvertUtil.longToBytes(root.getStartPos()), 264);
                FileUtil.writeDataIntoFile(indexFile, DataTypeConvertUtil.longToBytes(curSpaceOffset), 272);
            } else {
                InternalNode inParent = (InternalNode) retrieveNodeFromDisk(indexFile, in.getParentNodeAddr());
                inParent.insert(key, inLeft.getStartPos(), inRight.getStartPos()); // insert the key from child node into parent node, also add left child and right child pointers

                inLeft.setParentNodeAddr(inParent.getStartPos());
                inRight.setParentNodeAddr(inParent.getStartPos());

                if (inParent.getValueSize() > m) {
                    splitAndRebalance(in, indexFile);
                } else {
                    // write two leaf node and parent node into file
                    writeNodeIntoIndexFile(inLeft, indexFile);
                    writeNodeIntoIndexFile(inRight, indexFile);
                    writeNodeIntoIndexFile(inParent, indexFile);

                    // update current space offse
                    FileUtil.writeDataIntoFile(indexFile, DataTypeConvertUtil.longToBytes(curSpaceOffset), 272);
                }
            }
        }
    }

    /**
     * convert a node into byte array and then write the byte array into index file
     * @param node node
     * @param indexFile index file
     */
    private void writeNodeIntoIndexFile(Node node, String indexFile) {
        List<byte[]> bytesList = new ArrayList<>();
        byte[] leafNodeBytes = new byte[1];
        leafNodeBytes[0] = DataTypeConvertUtil.booleanToBytes(node.isLeafNode); // whether it is a leaf node
        byte[] keySizeBytes = new byte[1];
        keySizeBytes[0] = (byte) node.getKeysSize();
        byte[] valueSizeBytes = new byte[1];
        valueSizeBytes[0] = (byte) node.getValueSize();
        byte[] nextLeafAddrBytes = new byte[8];
        byte[] parentAddrBytes = DataTypeConvertUtil.longToBytes(node.getParentNodeAddr());
        if (node.isLeafNode) {
            LeafNode lf = (LeafNode) node;
            if (lf.nextLeaf == null)
                lf.nextLeaf = 0L;
            nextLeafAddrBytes = DataTypeConvertUtil.longToBytes(lf.nextLeaf);
        }
        // convert keys to byte array
        StringBuilder sb = new StringBuilder();
        for (String key : node.keys) {
            sb.append(key);
        }
        byte[] keysBytes = sb.toString().getBytes();

        bytesList.add(leafNodeBytes);
        bytesList.add(keySizeBytes);
        bytesList.add(valueSizeBytes);
        bytesList.add(nextLeafAddrBytes);
        bytesList.add(parentAddrBytes);
        bytesList.add(keysBytes);

        // convert values to byte array
        for (long value : node.values) {
            bytesList.add(DataTypeConvertUtil.longToBytes(value));
        }

        byte[] nodeBytes = new byte[1024];
        ByteBuffer buffer = ByteBuffer.wrap(nodeBytes);
        for (byte[] bytes : bytesList) {
            buffer.put(bytes);
        }

        FileUtil.writeDataIntoFile(indexFile, nodeBytes, node.getStartPos());
        
    }

    /**
     * public method, able to be invoked from outside the class
     * @param key key
     * @param indexFile index file
     * @return record starting position, -1 if the key is not found
     */
    public long find(String key, String indexFile) {
        if (root == null)
            // when root is null, set root, retrieve the position of root node in index file
            setRoot(indexFile);

        return find(root, key, indexFile);

    }

    /**
     * private method, used to recursively call itself to find whether the key is in index file
     * @param node node
     * @param key key
     * @param indexFile index
     * @return next node starting position or -1L if the key is not found
     */
    private long find(Node node, String key, String indexFile) {
        if (node == null)
            return -1L;

        if (node.isLeafNode) {
            // leaf node, find the key
            LeafNode lf = (LeafNode) node;
            int index = lf.keys.indexOf(key);
            if (index != -1)
                return lf.values.get(index);
            else
                return (long) index;
        } else {
            // internal node
            InternalNode in = (InternalNode) node;
            long pointer = -1L;
            if (key.compareTo(in.keys.get(in.keys.size() - 1)) > 0)
                pointer = in.values.get(in.values.size() - 1);
            else {
                for (int i = 0; i < in.keys.size(); i++) {
                    if (key.compareTo(in.keys.get(i)) <= 0) {
                        pointer = in.values.get(i);
                        break;
                    }
                }
            }

            Node nextNode = retrieveNodeFromDisk(indexFile, pointer);
            return find(nextNode, key, indexFile);
        }
    }

    /**
     * get the key-value pairs starts from the given key and the length is len
     * @param key key
     * @param n n
     * @param indexFile index file
     * @return a map to store n key-value pairs
     */
    public List<Long> traverseLeafNodes(String key, int n, String indexFile) {
        if (root == null)
            setRoot(indexFile);

        if (root == null)
            return null;

        LeafNode lf = (LeafNode) findLeaf(root, key, indexFile);

        return traverseLeafNodes(lf, key, n, indexFile);
    }

    /**
     * find the leaf node
     * @param node node
     * @param key key
     * @param indexFile index file
     * @return leaf node
     */
    private Node findLeaf(Node node, String key, String indexFile) {
        long pointer = -1L;
        if (node.isLeafNode) {
            return node;
        }
        else {
            InternalNode in = (InternalNode) node;
            if (key.compareTo(in.keys.get(in.keys.size() - 1)) > 0)
                pointer = in.values.get(in.values.size() - 1);
            else {
                for (int i = 0; i < in.keys.size(); i++) {
                    if (key.compareTo(in.keys.get(i)) <= 0) {
                        pointer = in.values.get(i);
                        break;
                    }
                }
            }

        }
        Node nextNode = retrieveNodeFromDisk(indexFile, pointer);
        return findLeaf(nextNode, key, indexFile);
    }

    /**
     * traverse leaf node, find the next n items starts from the key's position
     * @param lf leaf node
     * @param key key
     * @param n the number of items need to be traversed if possible
     * @param indexFile index file
     * @return record address list
     */
    private List<Long> traverseLeafNodes(LeafNode lf, String key, int n, String indexFile) {
        List<Long> addrList = new ArrayList<>();

        // leaf node, traverse the node starts from the key position, to find the next n items
        int keyPos = lf.keys.indexOf(key);
        if (keyPos == -1) {
            for (int i = 0; i < lf.keys.size(); i++) {
                if (key.compareTo(lf.keys.get(i)) < 0) {
                    addrList.add(lf.values.get(i)); // find the next larger key's related value
                    break;
                }
            }
        } else {
            int i = keyPos;
            while (addrList.size() < n) {
                if (i < lf.values.size()) {
                    addrList.add(lf.values.get(i));
                    i++; // ready to traverse the next value in lf.values
                } else {
                    i = 0; // set i = 0, ready to traverse the next leaf node
                    if (lf.nextLeaf != 0L)
                        lf = (LeafNode) retrieveNodeFromDisk(indexFile, lf.nextLeaf);
                    else
                        break; // no next leaf, return addrList
                }
            }
        }
        return addrList;
    }

    /**
     * retrieve 1k block and covert it to node
     * @param indexFile index file
     * @param startPos start position
     * @return Node instance
     */
    private Node retrieveNodeFromDisk(String indexFile, long startPos) {
        if (startPos == -1L)
            return null;

        byte[] nodeContent = FileUtil.getDataBlock(indexFile, 1024, startPos);
        boolean isLeafNode = (int) nodeContent[0] != 0; //TODO test whether this flag is assigned correctly
        int curKeyCount = (int) nodeContent[1]; // how many keys are currently in the node
        int curValueCount = (int) nodeContent[2]; // how many values(pointers) are currently in the node
        byte[] nextLeafAddrBytes = Arrays.copyOfRange(nodeContent, 3, 11);
        long nextLeafAddr = DataTypeConvertUtil.bytesToLong(nextLeafAddrBytes);  // next leaf node address
        byte[] parentNodeAddrBytes = Arrays.copyOfRange(nodeContent, 11, 19);
        long parentNodeAddr = DataTypeConvertUtil.bytesToLong(parentNodeAddrBytes); // parent node address
        ArrayList<String> keys = new ArrayList<>();
        ArrayList<Long> values = new ArrayList<>();
        int offset = 19; // 1(tree or leaf node flag) + 1(current key count) + 1(current value count) + 8(store next leaf address) + 8(store parent node address)
        for (int i = 0; i < curKeyCount; i++) {
            byte[] keyBytes = Arrays.copyOfRange(nodeContent, offset, offset + keySize);
            keys.add(new String(keyBytes));
            offset += keySize;
        }
        for (int i = 0; i < curValueCount; i++) {
            byte[] valueBytes = Arrays.copyOfRange(nodeContent, offset, offset + 8);
            values.add(DataTypeConvertUtil.bytesToLong(valueBytes));
            offset += 8; // retrieve 8 bytes pointer, thus offset plus 8 every time
        }

        Node node;
        if (isLeafNode)
            node = new LeafNode(keys, values, nextLeafAddr);
        else
            node = new InternalNode(keys, values);

        node.setParentNodeAddr(parentNodeAddr);
        node.setStartPos(startPos);

        return node;

    }

    /**
     * if root is null, retrieve root node from the disk
     * @param indexFile index file
     */
    private void setRoot(String indexFile) {
        long rootAddr = FileUtil.getRootAddr(indexFile);
        if (rootAddr != 0L)
            root = retrieveNodeFromDisk(indexFile, rootAddr);
    }

    /**
     * Tree node
     */
    private class InternalNode extends Node {

        public InternalNode(String key, long leftChild, long rightChild) {
            isLeafNode = false;
            keys = new ArrayList<>();
            keys.add(key);
            values = new ArrayList<>();
            values.add(leftChild);
            values.add(rightChild);
        }

        public InternalNode(List<String> keys, List<Long> values) {
            isLeafNode = false;
            this.keys = keys;
            this.values = values;
        }

        public void insert(String key, Long leftChild, long rightChild) {
            if (key.compareTo(keys.get(keys.size() - 1)) > 0) {
                keys.add(key);
                values.set(values.size() - 1, leftChild);
                values.add(rightChild);
            } else {
                for (int i = 0; i < keys.size(); i++) {
                    if (key.compareTo(keys.get(i)) <= 0) {
                        keys.add(i, key);
                        values.set(i, leftChild);
                        values.add(i + 1, rightChild);
                        break;
                    }
                }
            }
        }

        @Override
        public int getKeysSize() {
            return keys.size();
        }

        @Override
        public int getValueSize() {
            return values.size();
        }

        @Override
        public void setParentNodeAddr(long parentNodeAddr) {
            this.parentNodeAddr = parentNodeAddr;
        }

        @Override
        public long getParentNodeAddr() {
            return parentNodeAddr;
        }

        @Override
        public void setStartPos(long pos) {
            startPos = pos;
        }

        @Override
        public long getStartPos() {
            return startPos;
        }
    }

    /**
     * Leaf node
     */
    private class LeafNode extends Node {
        public Long nextLeaf;     // store the disk address of next leaf

        public LeafNode() {
            isLeafNode = true;
            keys = new ArrayList<>();
            values = new ArrayList<>();
        }

        public LeafNode(String firstKey, long firstValue) {
            isLeafNode = true;
            keys = new ArrayList<>();
            keys.add(firstKey);
            values = new ArrayList<>();
            values.add(firstValue);
        }

        public LeafNode(ArrayList<String> keys, ArrayList<Long> values) {
            isLeafNode = true;
            this.keys = keys;
            this.values = values;
        }

        public LeafNode(List<String> keys, List<Long> values, long nextLeaf) {
            isLeafNode = true;
            this.keys = keys;
            this.values = values;
            this.nextLeaf = nextLeaf;
        }

        public void setNextLeaf(long nextLeaf) {
            this.nextLeaf = nextLeaf;
        }

        @Override
        public int getKeysSize() {
            return keys.size();
        }

        @Override
        public int getValueSize() {
            return values.size();
        }

        @Override
        public void setParentNodeAddr(long addr) {
            parentNodeAddr = addr;
        }

        @Override
        public long getParentNodeAddr() {
            return parentNodeAddr;
        }

        @Override
        public void setStartPos(long pos) {
            startPos = pos;
        }

        @Override
        public long getStartPos() {
            return startPos;
        }

        /**
         * insert a key/value pair into LeafNode
         * @param key key
         * @param value value
         * @return true if insert succeeds, false if the key already exists, insert fails
         */
        public boolean insert(String key, long value) {
            if (keys.size() == 0 && values.size() == 0) {
                keys.add(key);
                values.add(value);
                return true;
            }

            if (key.compareTo(keys.get(0)) < 0) {
                keys.add(0, key);
                values.add(0, value);
            } else if (key.compareTo(keys.get(keys.size() - 1)) > 0) {
                keys.add(key);
                values.add(value);
            } else {
                // TODO after successfully insert an key-value pair, return true to jump out of the loop,
                // TODO otherwise it will end up as an infinite loop, since after every insertion, keys.size() is growing, and i will always less than keys.size()
                for (int i = 1; i < keys.size(); i++) {
                    if (key.compareTo(keys.get(i)) < 0) {
                        keys.add(i, key);
                        values.add(i, value);
                        return true;
                    } else if (key.compareTo(keys.get(i)) == 0)
                        return false;
                }
            }

            return true;
        }

    }

    /**
     * Node, has two subclasses: InternalNode and LeafNode
     */
    private abstract class Node {
        protected boolean isLeafNode;
        protected long parentNodeAddr;
        protected long startPos;
        protected List<String> keys;
        protected List<Long> values;
        public abstract int getKeysSize();
        public abstract int getValueSize();
        public abstract void setParentNodeAddr(long addr);
        public abstract long getParentNodeAddr();
        public abstract void setStartPos(long pos);
        public abstract long getStartPos();
    }
}
