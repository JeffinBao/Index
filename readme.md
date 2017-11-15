# Create index for txt file using b+ tree
---

## Basic Descrition
  - The txt file has a large amount of key-record pairs which are stored line by line.
  - The index file in created in blocks which is 1024 bytes per block. We use the key in txt file as the key in index file, and the start position(convert it to **long type**) of every record as the value stored in leaf nodes of the b+ tree. 
  - The header block of the index file has **256 bytes** storing the txt file name, **8 bytes** storing key length, **8 bytes** storing root node block's starting position and **8 bytes** next available block's starting offset.
  - Every internal node and leaf node of the b+ tree has the following structure: **1 byte** indicating whether it's internal node or leaf node, **1 byte** indicating current key size of the node(in the block), **1 byte** indicating current value size of the node, **8 bytes** indicating the next leaf node address and **8 bytes** indicating parent node address which is crucial when rebalancing the tree.
  - Since we konw the size of a block which is 1024 bytes and the key size should be provided, we are able to calculate the maximum tree pointer size of the internal node and key-value pair sieze of the leaf node.
  - When spliting and rebalancing the full node, we should use the next avaible block to store the new generated node. Also, we add the next leaf pointer to the new generate node in order to remember the next leaf pointer. 
  - Root node is not always in the same block position, when the root is spliting, the new root will also move to the next available node.
  - Every internal node may have multiple pointers, which are the start position, point to its child nodes.
  - Use RandomAccessFile class to read and write data starting at a specific position.
  - The program is now supporting creating new index file, inserting a new record, finding a record by key and listing the next n records strating from the given key. **Delete** function is currently not done yet. Work needs to be done maybe in the future, I don't know. This is really tough project for me as a rookie in CS field.




