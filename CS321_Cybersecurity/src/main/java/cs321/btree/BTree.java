package cs321.btree;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class BTree implements BTreeInterface {

    private static final int METADATA_SIZE = 40;
    private static final int KEY_BYTES = 64;

    private RandomAccessFile file;
    private final String filename;

    private int degree;
    private int maxKeys;
    private int maxChildren;
    private int nodeSize;

    private long rootOffset;
    private long nextFreeOffset;
    private long nodeCount;
    private long size;
    private int height;

    private BTreeNode root;


    // Constructors


    public BTree(String filename) throws BTreeException {
        this(2, filename);
    }

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
                throw new BTreeException("Degree must be >= 2 or 0.");
            }

            this.degree = actualDegree;
            this.maxKeys = 2 * actualDegree - 1;
            this.maxChildren = 2 * actualDegree;
            this.nodeSize = computeNodeSize();

            this.rootOffset = METADATA_SIZE;
            this.nextFreeOffset = rootOffset;
            this.nodeCount = 0;
            this.size = 0;
            this.height = 0;

            root = new BTreeNode(true);
            root.offset = nextFreeOffset;
            nextFreeOffset += nodeSize;
            nodeCount = 1;

            diskWrite(root);
            writeMetadata();

        } catch (IOException e) {
            throw new BTreeException("Error initializing BTree: " + e.getMessage());
        }
    }


    // Basic getters


    public long getSize() { return size; }
    public int getDegree() { return degree; }
    public long getNumberOfNodes() { return nodeCount; }
    public int getHeight() { return height; }


    // Insert


    public void insert(TreeObject obj) throws IOException {
        if (obj == null) return;

        BTreeNode r = root;
        boolean insertedNewKey;

        if (r.numKeys == maxKeys) {
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

        if (insertedNewKey) size++;
        writeMetadata();
    }

    // Insert into a non-full node
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
                } else if (cmp > 0) break;

                node.keys[i + 1] = node.keys[i];
                i--;
            }

            node.keys[i + 1] = new TreeObject(key, obj.getCount());
            node.numKeys++;
            diskWrite(node);
            return true;
        }

        // Internal node
        int i = node.numKeys - 1;
        while (i >= 0 && key.compareTo(node.keys[i].getKey()) < 0) i--;

        if (i >= 0 && key.equals(node.keys[i].getKey())) {
            node.keys[i].incCount();
            diskWrite(node);
            return false;
        }

        int childIndex = i + 1;
        BTreeNode child = diskRead(node.children[childIndex]);

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

            child = diskRead(node.children[childIndex]);
        }

        boolean inserted = insertNonFull(child, obj);
        diskWrite(child);
        return inserted;
    }


    // Split child


    private void splitChild(BTreeNode parent, int index) throws IOException {
        BTreeNode y = diskRead(parent.children[index]);
        BTreeNode z = new BTreeNode(y.isLeaf);

        z.offset = nextFreeOffset;
        nextFreeOffset += nodeSize;
        nodeCount++;

        int t = degree;

        // Move keys to z
        z.numKeys = t - 1;
        for (int j = 0; j < t - 1; j++) {
            z.keys[j] = y.keys[j + t];
            y.keys[j + t] = null;
        }

        // Move children if internal
        if (!y.isLeaf) {
            for (int j = 0; j < t; j++) {
                z.children[j] = y.children[j + t];
                y.children[j + t] = -1L;
            }
        }

        TreeObject midKey = y.keys[t - 1];
        y.keys[t - 1] = null;
        y.numKeys = t - 1;

        // Shift children
        for (int j = parent.numKeys; j >= index + 1; j--) {
            parent.children[j + 1] = parent.children[j];
        }
        parent.children[index + 1] = z.offset;

        // Shift keys
        for (int j = parent.numKeys - 1; j >= index; j--) {
            parent.keys[j + 1] = parent.keys[j];
        }
        parent.keys[index] = midKey;

        parent.numKeys++;

        diskWrite(y);
        diskWrite(z);
        diskWrite(parent);
        writeMetadata();
    }


    // Search


    public TreeObject search(String key) throws IOException {
        if (key == null || root == null) return null;
        return searchRecursive(root, key);
    }

    private TreeObject searchRecursive(BTreeNode node, String key) throws IOException {
        int i = 0;
        while (i < node.numKeys && key.compareTo(node.keys[i].getKey()) > 0) i++;

        if (i < node.numKeys && key.equals(node.keys[i].getKey()))
            return node.keys[i];

        if (node.isLeaf) return null;

        long child = node.children[i];
        if (child < 0) return null;

        return searchRecursive(diskRead(child), key);
    }


    // Dumping


    public void dumpToFile(PrintWriter out) throws IOException {
        if (out == null || size == 0) return;
        dumpInorderToWriter(rootOffset, out);
    }

    private void dumpInorderToWriter(long offset, PrintWriter out) throws IOException {
        if (offset < 0) return;

        BTreeNode node = diskRead(offset);

        int i;
        for (i = 0; i < node.numKeys; i++) {
            if (node.children[i] >= 0)
                dumpInorderToWriter(node.children[i], out);

            TreeObject t = node.keys[i];
            out.println(t.getKey() + " " + t.getCount());
        }

        if (node.children[i] >= 0)
            dumpInorderToWriter(node.children[i], out);
    }

    public void dumpToDatabase(String dbName, String tableName) throws IOException {
        if (dbName == null || tableName == null || size == 0) return;

        Connection conn = null;
        Statement stmt = null;
        PreparedStatement ps = null;

        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbName);
            conn.setAutoCommit(false);

            stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            stmt.executeUpdate(
                "CREATE TABLE " + tableName +
                " (key TEXT NOT NULL, frequency INTEGER NOT NULL)"
            );

            ps = conn.prepareStatement(
                "INSERT INTO " + tableName + " (key, frequency) VALUES (?, ?)"
            );

            dumpInorderToDatabase(rootOffset, ps);
            conn.commit();

        } catch (SQLException e) {
            try { if (conn != null) conn.rollback(); } catch (SQLException ignore) {}
            throw new IOException("Error writing database: " + e.getMessage());
        } finally {
            try { if (ps != null) ps.close(); } catch (SQLException ignore) {}
            try { if (stmt != null) stmt.close(); } catch (SQLException ignore) {}
            try { if (conn != null) conn.close(); } catch (SQLException ignore) {}
        }
    }

    private void dumpInorderToDatabase(long offset, PreparedStatement ps)
            throws IOException, SQLException {

        if (offset < 0) return;
        BTreeNode node = diskRead(offset);

        int i;
        for (i = 0; i < node.numKeys; i++) {
            if (node.children[i] >= 0)
                dumpInorderToDatabase(node.children[i], ps);

            TreeObject t = node.keys[i];
            ps.setString(1, t.getKey());
            ps.setLong(2, t.getCount());
            ps.addBatch();
        }

        if (node.children[i] >= 0)
            dumpInorderToDatabase(node.children[i], ps);

        ps.executeBatch();
    }


    // Sorted array for tests


    public String[] getSortedKeyArray() throws IOException {
        if (size == 0) return new String[0];

        List<String> keys = new ArrayList<>((int) Math.min(size, Integer.MAX_VALUE));
        inorder(rootOffset, keys);
        return keys.toArray(new String[0]);
    }

    private void inorder(long offset, List<String> out) throws IOException {
        if (offset < 0) return;

        BTreeNode node = diskRead(offset);

        int i;
        for (i = 0; i < node.numKeys; i++) {
            if (node.children[i] >= 0)
                inorder(node.children[i], out);

            out.add(node.keys[i].getKey());
        }

        if (node.children[i] >= 0)
            inorder(node.children[i], out);
    }


    // Disk I/O


    private int computeNodeSize() {
        int header = 1 + 4;
        int keys = maxKeys * (KEY_BYTES + 8);
        int children = maxChildren * Long.BYTES;
        return header + keys + children;
    }

    private int calculateOptimalDegree() {
        int best = 2;
        for (int t = 2; t < 1000; t++) {
            int mk = 2 * t - 1;
            int mc = 2 * t;

            int size = (1 + 4) + mk * (KEY_BYTES + 8) + mc * Long.BYTES;
            if (size > 4096) break;
            best = t;
        }
        return best;
    }

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
            byte[] keyBytes = new byte[KEY_BYTES];
            file.readFully(keyBytes);
            long count = file.readLong();

            if (i < numKeys && count > 0) {
                int len = 0;
                while (len < KEY_BYTES && keyBytes[len] != 0) len++;
                String key = new String(keyBytes, 0, len, StandardCharsets.UTF_8);
                node.keys[i] = new TreeObject(key, count);
            } else {
                node.keys[i] = null;
            }
        }

        for (int i = 0; i < maxChildren; i++)
            node.children[i] = file.readLong();

        return node;
    }

    public void diskWrite(BTreeNode node) throws IOException {
        file.seek(node.offset);

        file.writeBoolean(node.isLeaf);
        file.writeInt(node.numKeys);

        for (int i = 0; i < maxKeys; i++) {
            if (i < node.numKeys && node.keys[i] != null) {
                byte[] keyBytes = node.keys[i].getKey().getBytes(StandardCharsets.UTF_8);
                byte[] buf = new byte[KEY_BYTES];
                System.arraycopy(keyBytes, 0, buf, 0, Math.min(keyBytes.length, KEY_BYTES));
                file.write(buf);
                file.writeLong(node.keys[i].getCount());
            } else {
                file.write(new byte[KEY_BYTES]);
                file.writeLong(0L);
            }
        }

        for (int i = 0; i < maxChildren; i++)
            file.writeLong(node.children[i]);
    }


    // Inner Node class


    private class BTreeNode {
        boolean isLeaf;
        int numKeys;
        TreeObject[] keys;
        long[] children;
        long offset;

        BTreeNode(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.numKeys = 0;
            this.keys = new TreeObject[maxKeys];
            this.children = new long[maxChildren];
            for (int i = 0; i < maxChildren; i++)
                children[i] = -1L;
        }
    }

 
    // Close file


    public void close() throws IOException {
        if (file != null) {
            writeMetadata();
            file.close();
            file = null;
        }
    }

    @Override
    public void delete(String key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }
}
