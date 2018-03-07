import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import javafx.util.Pair;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Utility class which load all Database data to a Graph object.
 */

public class DatabaseDataLoader {
    // Connection to the db
    private Connection con;
    // Statement used for the query execution
    private Statement stmt;
    private PreparedStatement pstmt;
    // Result set used for queries
    private ResultSet rsTable;
    private ResultSet rs;
    // Graph to return
    private Graph g;
    // Query for number of tables
    private static final String NUMBER_TABLE_QUERY = "SELECT count(tablename) FROM pg_catalog.pg_tables WHERE schemaname = 'public';";
    // Query for names of tables
    private static final String TABLE_QUERY = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename;";
    // Query for total number of tuples
    private static final String NUMBER_TUPLE_QUERY = "SELECT sum(n_live_tup ) FROM pg_stat_user_tables;";
    // Query for number of tuples in a table
    private static final String NUMBER_TUPLE_TABLE_QUERY = "SELECT count(*) FROM ";
    // Query for columns of a table
    private static final String COLUMN_TABLE_QUERY = "SELECT column_name FROM information_schema.columns WHERE table_name = ?;";
    // Query for columns of a table
    private static final String PRIMARY_KEY_QUERY = "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = ? ::regclass AND i.indisprimary;";
    // Query for all tuples of a table
    private static final String TUPLE_QUERY = "SELECT * FROM ";
    // Query for get for minSearchId and maxSearchId
    private static final String SEARCH_ID_RANGE = "SELECT MIN(__search_id), MAX(__search_id) FROM ";
    // Query for foreign keys of a table: column name, external table, external column
    private static final String FOREIGN_KEY_QUERY = "SELECT " +
            "    att2.attname as \"child_column\", " +
            "    cl.relname as \"parent_table\", " +
            "    att.attname as \"parent_column\" " +
            "FROM " +
            "   (SELECT " +
            "        unnest(con1.conkey) AS \"parent\", " +
            "        unnest(con1.confkey) AS \"child\", " +
            "        con1.confrelid, " +
            "        con1.conrelid " +
            "    FROM " +
            "        pg_class cl " +
            "        join pg_namespace ns ON cl.relnamespace = ns.oid " +
            "        join pg_constraint con1 ON con1.conrelid = cl.oid " +
            "    WHERE " +
            "        cl.relname = ? " +
            "        AND ns.nspname = 'public' " +
            "        AND con1.contype = 'f' " +
            "   ) con " +
            "   JOIN pg_attribute att ON " +
            "       att.attrelid = con.confrelid and att.attnum = con.child " +
            "   JOIN pg_class cl ON " +
            "       cl.oid = con.confrelid " +
            "   JOIN pg_attribute att2 ON " +
            "       att2.attrelid = con.conrelid and att2.attnum = con.parent " +
            "ORDER BY parent_table;";

    /**
     * Work on the specified connection.
     *
     * @param con name of the connection to the database.
     */
    public DatabaseDataLoader(Connection con) {
        this.con = con;
        stmt = null;
        pstmt = null;
        rsTable = null;
        rs = null;
        g = null;
    }

    /**
     * Return a Graph containing all the records of the database.
     *
     * @return a Graph containing all the records of the database
     */
    public Graph getGraph(GUI gui) throws SQLException, IOException {
        // ArrayList for save the column's name of a table
        ArrayList<String> column;
        // HashMap for save the primary keys of a table
        HashMap<String, ArrayList<String>> primaryKeys;
        // LinkedList of idSearches of external tuples in a foreign key
        LinkedList<Integer> idSearch = null;
        // ArrayList for save the value of a tuple
        ArrayList<String> row;
        // To save the names of all tables
        String[] table;
        // To save all the foreign keys of a table
        HashMap<String, LinkedList<Pair<String, String>>> foreignKey;
        // Collection of idSearches of externals tuple in a foreign key
        Int2ObjectOpenHashMap idSearchLinks;
        // Other variables declaration
        int numberTable;
        int numberTuple;
        int numberTupleTable;
        int i;
        int currentId;
        int foreignId;
        int minSearchId;
        int maxSearchId;

        // Get number of total tuples
        stmt = con.createStatement();
        rs = stmt.executeQuery(NUMBER_TUPLE_QUERY);
        rs.next();
        numberTuple = rs.getInt(1);
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }

        // Get numbers of tables
        stmt = con.createStatement();
        rs = stmt.executeQuery(NUMBER_TABLE_QUERY);
        rs.next();
        numberTable = rs.getInt(1);
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }

        // Creation of the empty graph
        g = new Graph(numberTuple, gui);

        // Get name and primary keys of all tables
        primaryKeys = new HashMap<>();
        table = new String[numberTable];
        stmt = con.createStatement();
        rsTable = stmt.executeQuery(TABLE_QUERY);
        i = 0;
        while (rsTable.next()) {
            table[i] = rsTable.getString(1);
            // Get the columns which forms a primary key for this table
            pstmt = con.prepareStatement(PRIMARY_KEY_QUERY);
            pstmt.setString(1, table[i]);
            rs = pstmt.executeQuery();
            // For each column of this table
            while (rs.next()) {
                // Add the column in the list
                primaryKeys.putIfAbsent(table[i], new ArrayList<>());
                primaryKeys.get(table[i]).add(rs.getString(1));
            }
            i++;
        }
        if (rsTable != null) {
            rsTable.close();
        }
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }

        if (pstmt != null) {
            pstmt.close();
        }

        // For each table
        for (int j = 0; j < numberTable; j++) {
            idSearchLinks = new Int2ObjectOpenHashMap();
            column = new ArrayList<>();
            i = 0;
            // Get the numbers of tuple of this table
            stmt = con.createStatement();
            rs = stmt.executeQuery(NUMBER_TUPLE_TABLE_QUERY + table[j] + " ;");
            rs.next();
            numberTupleTable = rs.getInt(1);
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            // Get the range of the search id of this table
            stmt = con.createStatement();
            rs = stmt.executeQuery(SEARCH_ID_RANGE + table[j] + " ;");
            rs.next();
            minSearchId = rs.getInt(1);
            maxSearchId = rs.getInt(2);
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            // Get the names of all columns of this table
            pstmt = con.prepareStatement(COLUMN_TABLE_QUERY);
            pstmt.setString(1, table[j]);
            rs = pstmt.executeQuery();
            // For each column of this table
            while (rs.next()) {
                // Add the column in the list except the search id
                if (!rs.getString(1).equals("__search_id")) {
                    column.add(i++, rs.getString(1));
                }
            }
            if (rs != null) {
                rs.close();
            }
            if (pstmt != null) {
                pstmt.close();
            }
            // Add the table details in the graph
            g.addTable(table[j], numberTupleTable, minSearchId, maxSearchId, column);
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // Find foreign key's information for current table,
            pstmt = con.prepareStatement(FOREIGN_KEY_QUERY);
            pstmt.setString(1, table[j]);
            rs = pstmt.executeQuery();
            foreignKey = new HashMap<>();
            // For each foreign key of the table
            while (rs.next()) {
                String currentColumnName = rs.getString(1);
                String foreignColumnName = rs.getString(3);
                // If it's the first key for this referred table
                foreignKey.putIfAbsent(rs.getString(2), new LinkedList<>());
                // Insert the new reference to the list for the referred table
                foreignKey.get(rs.getString(2)).add(new Pair<>(currentColumnName, foreignColumnName));
            }
            if (rs != null) {
                rs.close();
            }
            if (pstmt != null) {
                pstmt.close();
            }
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // Fill idSearchLinks which contain links to other table for each node of this table
            stmt = con.createStatement();
            String currentTable = table[j];
            // For each foreign key's table find foreign key link
            for (String foreignTable : foreignKey.keySet()) {
                // If  foreign table's primary key is composed by one column
                if (primaryKeys.get(foreignTable).size() == 1) {
                    // For each column of the foreign key of the current table
                    for (Pair<String, String> foreignLink : foreignKey.get(foreignTable)) {
                        String currentColumn = foreignLink.getKey();
                        String foreignColumn = foreignLink.getValue();
                        String sql = "SELECT current_table.__search_id, foreign_table.__search_id FROM " + currentTable + " AS current_table INNER JOIN " + foreignTable + " AS foreign_table ON current_table." + currentColumn + " = foreign_table." + foreignColumn + ";";
                        //System.out.println(sql);
                        rs = stmt.executeQuery(sql);
                        // Store each founded link to the idSearchLinks structure
                        while (rs.next()) {
                            currentId = rs.getInt(1);
                            foreignId = rs.getInt(2);
                            idSearchLinks.putIfAbsent(currentId, new LinkedList<>());
                            ((LinkedList<Integer>) idSearchLinks.get(currentId)).add(foreignId);
                        }
                    }
                }
                // Else if foreign table's primary key is composed by more than one column
                else {
                    LinkedList<Pair<String, String>> foreignLinksOfCurrentTable = (LinkedList<Pair<String, String>>) foreignKey.get(foreignTable).clone();
                    String sql = "SELECT current_table.__search_id, foreign_table.__search_id FROM " + currentTable
                            + " AS current_table INNER JOIN " + foreignTable + " AS foreign_table ON ";
                    // Construct the query with all the needed foreign query for the current table
                    while (!foreignLinksOfCurrentTable.isEmpty()) {
                        String currentColumn = "";
                        String foreignColumn = "";
                        i = 1;
                        int columnIndexInCurrentTable = 0;
                        // For each primary key
                        sql += "(";
                        for (String primaryKey : primaryKeys.get(foreignTable)) {
                            columnIndexInCurrentTable = findIndex(primaryKey, foreignLinksOfCurrentTable);
                            currentColumn = foreignLinksOfCurrentTable.get(columnIndexInCurrentTable).getKey();
                            foreignColumn = foreignLinksOfCurrentTable.get(columnIndexInCurrentTable).getValue();
                            if (i == primaryKeys.get(foreignTable).size()) {
                                break;
                            }
                            sql += "current_table." + currentColumn + " = foreign_table." + foreignColumn + " AND ";
                            foreignLinksOfCurrentTable.remove(columnIndexInCurrentTable);
                            i++;
                        }
                        sql += "current_table." + currentColumn + " = foreign_table." + foreignColumn + ")";
                        foreignLinksOfCurrentTable.remove(columnIndexInCurrentTable);
                        // If there is other element
                        if (!foreignLinksOfCurrentTable.isEmpty()) {
                            sql += " OR ";
                        }
                    }
                    sql += ";";
                    // Execute the query
                    rs = stmt.executeQuery(sql);
                    // Store each founded link to the idSearchLinks structure
                    while (rs.next()) {
                        currentId = rs.getInt(1);
                        foreignId = rs.getInt(2);
                        idSearchLinks.putIfAbsent(currentId, new LinkedList<>());
                        ((LinkedList<Integer>) idSearchLinks.get(currentId)).add(foreignId);
                    }
                }
            }
            // Free resources
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // Get all tuple of current table and add them to the graph
            stmt = con.createStatement();
            rs = stmt.executeQuery(TUPLE_QUERY + table[j] + ";");
            // for each tuple of the table
            while (rs.next()) {
                // new empty tuple
                row = new ArrayList<>();
                // for each column of the tuple
                for (int l = 1; l <= column.size() + 1; l++) {
                    // insert element in the tuple
                    row.add(rs.getString(l));
                }
                currentId = rs.getInt(column.size() + 1);

                // Get the list of search id to build all edges
                if (idSearchLinks.containsKey(currentId)) {
                    idSearch = (LinkedList<Integer>) idSearchLinks.get(currentId);
                } else {
                    idSearch = new LinkedList<>();
                }

                // Add the node in the graph
                g.addNode(table[j], currentId, row, idSearch);
            }
            // Free resources
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
        // Save the file containing all the words positions inside the graph
        g.closeIndexFile();
        return g;
    }

    /**
     * Find the index of the given word inside a linked list of pair.
     *
     * @param primaryKey  word to find
     * @param foreignKeys linked list of pairs (column name of current table, column name of foreign table)
     * @return
     */
    private int findIndex(String primaryKey, LinkedList<Pair<String, String>> foreignKeys) {
        int index = -1;
        for (int i = 0; i < foreignKeys.size(); i++) {
            if (foreignKeys.get(i).getValue().equals(primaryKey)) {
                index = i;
                break;
            }
        }
        return index;
    }
}