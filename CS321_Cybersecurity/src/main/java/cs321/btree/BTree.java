package cs321.btree;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class BTree implements BTreeInterface {

    private static final int BLOCK_SIZE = 4096;


    private static final int METADATA_SIZE = 40;



    private RandomAccessFile file;
    private final String filename;


    private int degree;        // t
    private int maxKeys;       // 2t - 1
    private int maxChildren;   // 2t
    private int nodeSize;      // bytes per node on disk


    private long rootOffset;
    private long nextFreeOffset;
    private long nodeCount;    // number of nodes in the tree
    private long size;         // number of distinct keys (TreeObjects)
    private int height;        // 0 when only root, increases on root splits

   
    private BTreeNode root;

    // constructor

    
    // Sets degree to 2 by default
    public BTree(String filename) throws BTreeException {
        this(2, filename);
    }

    // sets degree to passed into value and resets the file
    public BTree(int degree, String filename) throws BTreeException {
        this.filename = filename;

        try {
            this.file = new RandomAccessFile(filename, "rw");

            
            file.setLength(0);

            
            int actualDegree;
            if (degree == 0) {
                actualDegree = calculateOptimalDegree();
            } else if (degree >= 2) {
                actualDegree = degree;
            } else {
                throw new BTreeException("Degree must be >= 2 or 0 (auto).");
            }

            this.degree = actualDegree;
            this.maxKeys = 2 * actualDegree - 1;
            this.maxChildren = 2 * actualDegree;
            this.nodeSize = computeNodeSize();

           
            this.rootOffset = METADATA_SIZE;
            this.nextFreeOffset = this.rootOffset;
            this.nodeCount = 0;
            this.size = 0;
            this.height = 0;

            
            this.root = new BTreeNode(true);
            this.root.offset = this.nextFreeOffset;
            this.nextFreeOffset += this.nodeSize;
            this.nodeCount = 1;

           
            diskWrite(this.root);
            writeMetadata();

        } catch (IOException e) {
            throw new BTreeException("I/O error in BTree constructor: " + e.getMessage());
        }
    }



    // getters and setters
    public long getSize() {
        return size;
    }


    public int getDegree() {
        return degree;
    }


    public long getNumberOfNodes() {
        return nodeCount;
    }


    public int getHeight() {
        return height;
    }




    public void insert(TreeObject obj) throws IOException {
        if (obj == null) {
            return;
        }
        if (root == null) {
            throw new IllegalStateException("BTree root is not initialized.");
        }

        BTreeNode r = root;
        boolean insertedNewKey;

        if (r.numKeys == maxKeys) {
            // Root is full and then split it
            BTreeNode s = new BTreeNode(false);
            s.offset = nextFreeOffset;
            nextFreeOffset += nodeSize;
            nodeCount++;

            s.children[0] = r.offset;
            root = s;
            rootOffset = s.offset;
            height++;

            diskWrite(r);
            splitChild(s, 0);

            insertedNewKey = insertNonFull(s, obj);
            diskWrite(s);
        } else {
            insertedNewKey = insertNonFull(r, obj);
            diskWrite(r);
        }

        if (insertedNewKey) {
            size++;
        }

        writeMetadata();
    }


    public TreeObject search(String key) throws IOException {
        if (root == null || key == null) {
            return null;
        }
        return searchRecursive(root, key);
    }


    public void delete(String key) {
        // Not implemented 
    }


    public void dumpToFile(PrintWriter out) throws IOException {
        if (out == null) {
            return;
        }
        if (size == 0 || root == null) {
            return;
        }
        dumpInorderToWriter(rootOffset, out);
        out.flush();
    }


    public void dumpToDatabase(String dbName, String tableName) throws IOException {
        if (dbName == null || tableName == null || dbName.isEmpty() || tableName.isEmpty()) {
            return;
        }
        if (size == 0 || root == null) {
            return;
        }

        Connection conn = null;
        Statement stmt = null;
        PreparedStatement ps = null;
        try {
            String url = "jdbc:sqlite:" + dbName;
            conn = DriverManager.getConnection(url);
            conn.setAutoCommit(false);

            stmt = conn.createStatement();

            // Drop table if exists, then create new
            String dropSql = "DROP TABLE IF EXISTS " + tableName;
            stmt.executeUpdate(dropSql);

            String createSql = "CREATE TABLE " + tableName +
                    " (key TEXT NOT NULL, frequency INTEGER NOT NULL)";
            stmt.executeUpdate(createSql);

            String insertSql = "INSERT INTO " + tableName + " (key, frequency) VALUES (?, ?)";
            ps = conn.prepareStatement(insertSql);

            dumpInorderToDatabase(rootOffset, ps);

            conn.commit();

        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ignore) {}
            }
            throw new IOException("Error dumping BTree to database: " + e.getMessage(), e);
        } finally {
            if (ps != null) {
                try { ps.close(); } catch (SQLException ignore) {}
            }
            if (stmt != null) {
                try { stmt.close(); } catch (SQLException ignore) {}
            }
            if (conn != null) {
                try { conn.close(); } catch (SQLException ignore) {}
            }
        }
    }


    public String[] getSortedKeyArray() throws IOException {
        if (size == 0 || root == null) {
            return new String[0];
        }

        List<String> keys = new ArrayList<>((int) Math.min(size, Integer.MAX_VALUE));
        inorder(rootOffset, keys);
        return keys.toArray(new String[0]);
    }

    // b tree stuff

    /** Recursive search starting from a node already in memory. */
    private TreeObject searchRecursive(BTreeNode node, String key) throws IOException {
        int i = 0;

        while (i < node.numKeys && key.compareTo(node.keys[i].getKey()) > 0) {
            i++;
        }

        if (i < node.numKeys && key.equals(node.keys[i].getKey())) {
            return node.keys[i];
        }

        if (node.isLeaf) {
            return null;
        }

        long childOffset = node.children[i];
        if (childOffset < 0) {
            return null;
        }

        BTreeNode child = diskRead(childOffset);
        return searchRecursive(child, key);
    }

    /**
     * Insert a key into a node that is guaranteed NOT to be full.
     * Returns true if a new distinct key was inserted, false if a duplicate
     * was found and its count was incremented.
     */
    private boolean insertNonFull(BTreeNode node, TreeObject obj) throws IOException {
        String key = obj.getKey();

        if (node.isLeaf) {
            int i = node.numKeys - 1;

            while (i >= 0) {
                int cmp = key.compareTo(node.keys[i].getKey());
                if (cmp == 0) {
                    node.keys[i].incCount();
                    diskWrite(node);
                    return false;
                } else if (cmp > 0) {
                    break;
                }
                node.keys[i + 1] = node.keys[i];
                i--;
            }

            node.keys[i + 1] = new TreeObject(key, obj.getCount());
            node.numKeys++;
            diskWrite(node);
            return true;

        } else {
            int i = node.numKeys - 1;
            while (i >= 0 && key.compareTo(node.keys[i].getKey()) < 0) {
                i--;
            }

            if (i >= 0 && key.equals(node.keys[i].getKey())) {
                node.keys[i].incCount();
                diskWrite(node);
                return false;
            }

            int childIndex = i + 1;
            long childOffset = node.children[childIndex];
            BTreeNode child = diskRead(childOffset);

            if (child.numKeys == maxKeys) {
                splitChild(node, childIndex);

                int cmpMid = key.compareTo(node.keys[childIndex].getKey());
                if (cmpMid == 0) {
                    node.keys[childIndex].incCount();
                    diskWrite(node);
                    return false;
                } else if (cmpMid > 0) {
                    childIndex++;
                }

                childOffset = node.children[childIndex];
                child = diskRead(childOffset);
            }

            boolean inserted = insertNonFull(child, obj);
            diskWrite(child);
            return inserted;
        }
    }

    /**
     * Split the full child node at parent.children[index].
     * Parent is not full when this is called.
     */
    private void splitChild(BTreeNode parent, int index) throws IOException {
        long childOffset = parent.children[index];
        BTreeNode y = diskRead(childOffset);        // full child
        BTreeNode z = new BTreeNode(y.isLeaf);      // new sibling

        z.offset = nextFreeOffset;
        nextFreeOffset += nodeSize;
        nodeCount++;

        z.numKeys = degree - 1;
        for (int j = 0; j < degree - 1; j++) {
            z.keys[j] = y.keys[j + degree];
        }

        if (!y.isLeaf) {
            for (int j = 0; j < degree; j++) {
                z.children[j] = y.children[j + degree];
                y.children[j + degree] = -1L;
            }
        }

        y.numKeys = degree - 1;

        for (int j = parent.numKeys; j >= index + 1; j--) {
            parent.children[j + 1] = parent.children[j];
        }
        parent.children[index + 1] = z.offset;

        for (int j = parent.numKeys - 1; j >= index; j--) {
            parent.keys[j + 1] = parent.keys[j];
        }

        parent.keys[index] = y.keys[degree - 1];
        y.keys[degree - 1] = null;

        parent.numKeys++;

        diskWrite(y);
        diskWrite(z);
        diskWrite(parent);
        writeMetadata();
    }

    
    // In-order traversal helpers
    

    private void inorder(long nodeOffset, List<String> out) throws IOException {
        if (nodeOffset < 0) return;

        BTreeNode node = diskRead(nodeOffset);

        int i;
        for (i = 0; i < node.numKeys; i++) {
            if (!node.isLeaf) {
                long childOffset = node.children[i];
                if (childOffset >= 0) {
                    inorder(childOffset, out);
                }
            }
            out.add(node.keys[i].getKey());
        }

        if (!node.isLeaf) {
            long lastChild = node.children[i];
            if (lastChild >= 0) {
                inorder(lastChild, out);
            }
        }
    }

    private void dumpInorderToWriter(long nodeOffset, PrintWriter out) throws IOException {
        if (nodeOffset < 0) return;

        BTreeNode node = diskRead(nodeOffset);

        int i;
        for (i = 0; i < node.numKeys; i++) {
            if (!node.isLeaf) {
                long childOffset = node.children[i];
                if (childOffset >= 0) {
                    dumpInorderToWriter(childOffset, out);
                }
            }
            TreeObject t = node.keys[i];
            out.println(t.getKey() + " " + t.getCount());
        }

        if (!node.isLeaf) {
            long lastChild = node.children[i];
            if (lastChild >= 0) {
                dumpInorderToWriter(lastChild, out);
            }
        }
    }

    private void dumpInorderToDatabase(long nodeOffset, PreparedStatement ps) throws IOException, SQLException {
        if (nodeOffset < 0) return;

        BTreeNode node = diskRead(nodeOffset);

        int i;
        for (i = 0; i < node.numKeys; i++) {
            if (!node.isLeaf) {
                long childOffset = node.children[i];
                if (childOffset >= 0) {
                    dumpInorderToDatabase(childOffset, ps);
                }
            }
            TreeObject t = node.keys[i];
            ps.setString(1, t.getKey());
            ps.setLong(2, t.getCount());
            ps.addBatch();
        }

        if (!node.isLeaf) {
            long lastChild = node.children[i];
            if (lastChild >= 0) {
                dumpInorderToDatabase(lastChild, ps);
            }
        }

        // Execute batches occasionally to avoid huge memory spikes
        ps.executeBatch();
    }


    /** Compute bytes required for storing one node on disk. */
    private int computeNodeSize() {

        int headerSize = 1 + 4;  // boolean isLeaf, int numKeys

        int keyRecordSize = 64 + 8; // 64-byte padded key + 8 byte count
        int keysSectionSize = maxKeys * keyRecordSize;

        int childrenSectionSize = maxChildren * Long.BYTES;

        int baseSize = headerSize + keysSectionSize + childrenSectionSize;

        // Pad to next 4096-byte block
        int block = 4096;
        int padding = (block - (baseSize % block)) % block;

        return baseSize + padding;
    }


    /**
     * Calculate optimal degree such that a node fits into a 4096-byte block
     * as full as possible given the TreeObject.BYTES and child pointer size.
     */
    private int calculateOptimalDegree() {
        int best = 2; // minimum allowed

        for (int t = 2; t < 1000; t++) {
            int mk = 2 * t - 1;
            int mc = 2 * t;

            int headerSize = 1 + 4;          // isLeaf + numKeys
            int keyRecordSize = 64 + 8;      // key bytes + count
            int keysSectionSize = mk * keyRecordSize;
            int childrenSectionSize = mc * Long.BYTES;

            int baseSize = headerSize + keysSectionSize + childrenSectionSize;

            if (baseSize > BLOCK_SIZE) {
                break;
            }

            best = t;
        }

        return best;
}
    // Write metadata at the beginning of the file. 
    private void writeMetadata() throws IOException {
        file.seek(0);
        file.writeLong(rootOffset);
        file.writeLong(nextFreeOffset);
        file.writeInt(degree);
        file.writeLong(nodeCount);
        file.writeLong(size);
        file.writeInt(height);
    }


    public BTreeNode diskRead(long offset) throws IOException {
        file.seek(offset);

        boolean isLeaf = file.readBoolean();
        int numKeys = file.readInt();

        BTreeNode node = new BTreeNode(isLeaf);
        node.offset = offset;
        node.numKeys = numKeys;

        for (int i = 0; i < maxKeys; i++) {
            byte[] keyBytes = new byte[64];
            file.readFully(keyBytes);
            long count = file.readLong();

            if (i < numKeys && count > 0) {
                int len = 0;
                while (len < 64 && keyBytes[len] != 0) len++;
                String key = new String(keyBytes, 0, len, StandardCharsets.UTF_8);
                node.keys[i] = new TreeObject(key, count);
            } else {
                node.keys[i] = null;
            }
        }

        for (int i = 0; i < maxChildren; i++) {
            long child = file.readLong();
            node.children[i] = child;
        }

        return node;
    }


    /** Write a BTreeNode to disk at its offset. */
    public void diskWrite(BTreeNode node) throws IOException {
        file.seek(node.offset);

        // Write header
        file.writeBoolean(node.isLeaf);
        file.writeInt(node.numKeys);

        // Write fixed-length key entries
        for (int i = 0; i < maxKeys; i++) {
            if (i < node.numKeys && node.keys[i] != null) {
                byte[] keyBytes = node.keys[i].getKey().getBytes(StandardCharsets.UTF_8);
                byte[] buf = new byte[64];
                int len = Math.min(keyBytes.length, buf.length);
                System.arraycopy(keyBytes, 0, buf, 0, len);
                file.write(buf);
                file.writeLong(node.keys[i].getCount());
            } else {
                // write empty record
                file.write(new byte[64]);
                file.writeLong(0L);
            }
        }

        // Write children offsets (always fixed count)
        for (int i = 0; i < maxChildren; i++) {
            long child = (node.children[i] >= 0 ? node.children[i] : -1L);
            file.writeLong(child);
        }

        // Pad to full nodeSize
        long written = file.getFilePointer() - node.offset;
        long padding = nodeSize - written;
        for (long i = 0; i < padding; i++) {
            file.writeByte(0);
        }
    }


    // ---------------------------------------------------------------
    // Inner node class
    // ---------------------------------------------------------------

    /** Inner BTreeNode class representing a node stored on disk. */
    private class BTreeNode {
        boolean isLeaf;
        int numKeys;
        TreeObject[] keys;
        long[] children;
        long offset; // disk offset

        BTreeNode(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.numKeys = 0;
            this.keys = new TreeObject[maxKeys];
            this.children = new long[maxChildren];
            for (int i = 0; i < maxChildren; i++) {
                this.children[i] = -1L;
            }
        }
    }

    // ---------------------------------------------------------------
    // Optional: close method
    // ---------------------------------------------------------------

    /**
     * Flush metadata and close the underlying file.
     */
    public void close() throws IOException {
        if (file != null) {
            writeMetadata();
            file.close();
            file = null;
        }
    }
}
