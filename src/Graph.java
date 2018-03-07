import it.unimi.dsi.fastutil.ints.*;
import javafx.util.Pair;
import javax.naming.LimitExceededException;
import java.io.*;
import java.util.*;
import java.util.Scanner;


/**
 * Implementation of a Graph required by the algorithm.
 */

public class Graph {

    // Constants declaration
    private final int DEFAULT_DIRECTED_EDGE_WEIGHT = 1;
    public static final String AUX_FILE_PATH = "." + File.separator + "output_files" + File.separator + "wIndex.txt";

    // Variables declaration
    private int nNodes;
    private int insertedNodes;
    private int insertedNodesForCurrentTable;
    private int progress;
    private BufferedWriter output;
    private int maxWeight;

    /**
     * Structure used to calculate backward edges weight
     */
    private Int2ObjectOpenHashMap<ParentsEdgesContainer> originParentEdges;
    /**
     * Store searchId of each node
     */
    public Int2ObjectOpenHashMap<GraphNode> nodes;

    /**
     * Store tables column names
     */
    private HashMap<String, TableDefinition> tables;

    private GUI gui;

    /**
     * Creates a Graph with a specified number of nodes.
     *
     * @param nNodes numbers of nodes of the graph
     */
    public Graph(int nNodes, GUI gui) throws IOException {
        File f = new File("." + File.separator + "output_files");
        if (!f.exists()) f.mkdir();
        nodes = new Int2ObjectOpenHashMap<>();
        System.out.println("Nodes to add: " + nNodes);
        tables = new HashMap<>();
        this.nNodes = nNodes;
        insertedNodes = 0;
        insertedNodesForCurrentTable = 0;
        output = createIndexFile();
        progress = 0;
        GUI.maxMemoryUsed = 0;
        originParentEdges = new Int2ObjectOpenHashMap<>();
        Runtime runtime = Runtime.getRuntime();
        long mb = 1024 * 1024;
        maxWeight = DEFAULT_DIRECTED_EDGE_WEIGHT;
        int count = 0;
        this.gui = gui;
    }

    /**
     * Add a table to tables definition hashmap.
     *
     * @param tableName   the name of the table
     * @param minSearchId
     * @param maxSearchId
     * @param column      the ArrayList containing all the columns information
     */
    public void addTable(String tableName, int nElements, int minSearchId, int maxSearchId, ArrayList<String> column) {
        tables.put(tableName.toLowerCase(), new TableDefinition(nElements, minSearchId, maxSearchId, column));
    }

    /**
     * Insert a new Node to the graph.
     *
     * @param tableName name of the table of the node
     */
    public void addNode(String tableName, int searchId, ArrayList<String> words, LinkedList<Integer> foreignKeyReferences) throws IOException {
        if (insertedNodes < nNodes) {
            // Insert the new node if it's not already present
            nodes.putIfAbsent(searchId, new GraphNode(searchId));
            // Save keywords positions
            writeToIndexFile(words, searchId, output);
            insertedNodes++;
            gui.updateUsedMemoryStats();
            if (progress < (int) (((double) (insertedNodes) / (double) nNodes) * 100)) {
                progress = (int) (((double) (insertedNodes) / (double) nNodes) * 100);
                gui.setLoadingGraphProgress(progress);
            }

            insertedNodesForCurrentTable++;
            // Add parent edge for destination nodes
            for (Integer destination : foreignKeyReferences) {
                // If referred node is not present add it
                nodes.putIfAbsent(destination, new GraphNode(destination));
                //add node to parent list
                GraphNode destNode = nodes.get(destination);
                destNode.addParentNode(searchId, DEFAULT_DIRECTED_EDGE_WEIGHT);
                // Increment referred node weight
                destNode.incrementNodeWeight();
                // Update max weight of the nodes in the graph
                if (maxWeight < destNode.getNodeWeight()) {
                    maxWeight = destNode.getNodeWeight();
                }
                // if node destination has been never reached before store it to calculate backward edge weight
                originParentEdges.putIfAbsent(destination, new ParentsEdgesContainer(destination));
                // Increment weight of the backward edge and save current node
                originParentEdges.get(destination).addOriginNode(searchId);
            }
            // Add backwards edges after all directed edges is present for this table
            if (tables.get((tableName)).getSize() == insertedNodesForCurrentTable) {
                // For each edge added for this table
                for (ParentsEdgesContainer parentEdge : originParentEdges.values()) {
                    // For each origin node pointing this node add the backward edge
                    for (Integer originNode : parentEdge.getOriginNodes()) {
                        //add parent connected by bkw edge
                        GraphNode tmp = nodes.get(originNode);
                        tmp.addParentNode(parentEdge.getDestinationNode(), parentEdge.getWeight());
                    }
                }
                originParentEdges.clear();
                insertedNodesForCurrentTable = 0;
            }
        }

    }


    /**
     * Return the list of iterators that starts from nodes which contains specified keywords.
     *
     * @param keywords array containing all the keywords
     */
    public PriorityQueue<GraphIterator> getIterators(String[] keywords) throws FileNotFoundException, LimitExceededException {
        long startCreateIteratorsTime = System.currentTimeMillis();
        PriorityQueue<GraphIterator> iterators = new PriorityQueue<>();
        String outputFileAbsolutePath = AUX_FILE_PATH;
        Scanner read = new Scanner(new File(outputFileAbsolutePath));
        Int2ObjectOpenHashMap<GraphIterator> iteratorsAlreadyAdded = new Int2ObjectOpenHashMap<>();
        HashSet<String> tablesAdded = new HashSet<>();
        long itStartCreationTime;
        int iteratorIndex = 1;
        int nTokensFound = 0;
        GraphIterator it;
        System.out.println("\nGenerating all the iterators...");
        // For each keyword check table's name and column's name
        for (String token : keywords) {
            ArrayList<String> tokenKeywords = new ArrayList<>();
            tokenKeywords.add(token);
            token = token.toLowerCase();

            // For each table
            for (String currentTableName : tables.keySet()) {
                LinkedList<Integer> nodesInRange;
                // If match with one of the table's name
                if (currentTableName.equals(token)) {
                    tablesAdded.add(currentTableName);
                    nodesInRange = getNodesInRange(tables.get(token).getMinSearchId(), tables.get(token).getMaxSearchId());
                    //Add a new iterator to the list, one for each node of that table
                    for (int searchId : nodesInRange) {
                        itStartCreationTime = System.currentTimeMillis();
                        ArrayList<String> tokens = (ArrayList<String>) tokenKeywords.clone();
                        it = new GraphIterator(searchId, tokens, 1);
                        iterators.add(it);
                        iteratorsAlreadyAdded.put(searchId, it);
                        gui.updateUsedMemoryStats();
                        // Check time
                        gui.checkTime(startCreateIteratorsTime);
                        // Check max memory
                        gui.checkMemory();
                        iteratorIndex++;
                    }
                }
                // Check columns of current table
                for (String columnName : tables.get(currentTableName).getColumns()) {
                    // If match with one of the columns's name
                    if (columnName.equals(token)) {
                        //System.out.println("*************************************************************************************\n");
                        nodesInRange = getNodesInRange(tables.get(currentTableName).getMinSearchId(), tables.get(currentTableName).getMaxSearchId());
                        // If current table has already been added update the corresponding iterator
                        if (tablesAdded.contains(currentTableName)) {
                            for (int searchId : nodesInRange) {
                                iteratorsAlreadyAdded.get(searchId).addTokens(tokenKeywords, 1);
                                gui.updateUsedMemoryStats();
                            }
                        }
                        // Else add an iterator for each node of this table
                        else {
                            for (int searchId : nodesInRange) {
                                itStartCreationTime = System.currentTimeMillis();
                                it = new GraphIterator(searchId, (ArrayList<String>) tokenKeywords.clone(), 1);
                                iterators.add(it);
                                iteratorsAlreadyAdded.put(searchId, it);
                                gui.updateUsedMemoryStats();
                                // Check time
                                gui.checkTime(startCreateIteratorsTime);
                                // Check max memory
                                gui.checkMemory();
                                iteratorIndex++;
                            }
                        }
                    }
                }
            }
        }
        //look each tuple in index file
        while (read.hasNext()) {
            String line = read.nextLine();
            String currKws = line.split("\t")[0];
            currKws = currKws.replace("[", "");
            currKws = currKws.replace(",", "");
            currKws = currKws.replace("]", "");
            currKws = currKws.replaceAll("[^a-zA-Z0-9 ]", "");
            currKws = currKws.toLowerCase();
            while (!line.contains("\t") && read.hasNext()) {
                line += read.nextLine();
            }
            int RID = Integer.parseInt(line.split("\t")[1]);
            ArrayList<String> tokenKeywords = new ArrayList<>();
            nTokensFound = 0;
            int nCurrentToken;
            // For each keyword
            for (int i = 0; i < keywords.length; i++) {
                String token = keywords[i];
                // Add a new token to the list if it is found
                token = token.toLowerCase();
                nCurrentToken = containedKey(currKws, token);
                nTokensFound += nCurrentToken;
                if (nCurrentToken > 0) {
                    tokenKeywords.add(token);
                }
            }
            if (!tokenKeywords.isEmpty()) {
                // Check if iterator for current search id has already been added
                if (!iteratorsAlreadyAdded.containsKey(RID)) {
                    itStartCreationTime = System.currentTimeMillis();
                    iterators.add(new GraphIterator(RID, tokenKeywords, nTokensFound));
                    gui.updateUsedMemoryStats();
                    // Check time
                    gui.checkTime(startCreateIteratorsTime);
                } else {// update iterator already added
                    iteratorsAlreadyAdded.get(RID).addTokens(tokenKeywords, nTokensFound);
                    gui.updateUsedMemoryStats();
                }
                // Check max memory
                gui.checkMemory();
                // Check time
                gui.checkTime(startCreateIteratorsTime);
                iteratorIndex++;
            }
            gui.updateUsedMemoryStats();
        }
        // Free resources
        iteratorsAlreadyAdded = null;
        tablesAdded = null;
        System.gc();
        return iterators;
    }

    /**
     * Return the max weight of all the nodes in the graph.
     *
     * @return max weight of all the nodes in the graph
     */
    public int getMaxNodeWeight() {
        return maxWeight;
    }

    /**
     * Test method 1
     */
    public void printTables() {
        System.out.println("TABLES:");
        for (String table : tables.keySet()) {
            System.out.print(table + " (");
            String temp = "";
            for (String column : tables.get(table).getColumns()) {
                temp += column + ", ";
            }
            System.out.println(temp.substring(0, temp.length() - 2) + ") DIMENSION: " + tables.get(table).getSize());
        }
        System.out.println();
    }

    /**
     * Test method 2
     */
    public void printNodes() {
        System.out.println("NODES: ");
        for (Object o : nodes.values()) {
            GraphNode node = (GraphNode) o;
            System.out.println("searchId: " + node.getSearchId() + " weight: " + node.getNodeWeight());
        }
        System.out.println();
    }

    /**
     * Test method 3
     */
    public void printEdges() {
        System.out.println("EDGES: nodeIndex -> (node, weight) (node, weight) ...");
        for (Object o : nodes.values()) {
            GraphNode node = (GraphNode) o;
            System.out.print(node.getSearchId() + " -> ");
            for (GraphNode.AdjElement edge : node.getParentsList()) {
                System.out.print("(" + edge.getAdjNodeSearchId() + ", " + edge.getEdgeWeight() + ") ");
            }
            System.out.println("");
        }
        System.out.println();
    }

    /**
     * Test method 4
     *
     * @param fileName name of the file to create
     */
    public void printEdgesToFile(String fileName) {
        try {
            File f = new File(fileName);
            if (f.exists()) {
                f.delete();
            }
            FileWriter fw = new FileWriter(new File(fileName));
            for (Object o : nodes.values()) {
                GraphNode node = (GraphNode) o;
                fw.append(node.getSearchId() + " -> ");
                for (GraphNode.AdjElement edge : node.getParentsList()) {
                    fw.append("(" + edge.getAdjNodeSearchId() + ", " + edge.getEdgeWeight() + ") ");
                }
                fw.append("\n");
            }
            fw.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

    }

    /**
     * Looks in tuple if there is the token character
     *
     * @param tuple the tuple in which to look in
     * @param token the toke to be searched
     */
    private int containedKey(String tuple, String token) {
        String[] tupleTokens = tuple.split("\\s+");
        int nTokensFound = 0;
        for (int i = 0; i < tupleTokens.length; i++) {
            if (tupleTokens[i].equals(token)) {
                nTokensFound++;
            }
        }
        return nTokensFound;
    }

    /**
     * Return all the nodes whose searchId is between the interval.
     *
     * @param min min value of the interval
     * @param max max value of the interval
     * @return list of nodes which searchId is between the interval
     */
    private LinkedList<Integer> getNodesInRange(int min, int max) {
        LinkedList<Integer> nodesList = new LinkedList<>();
        for (int searchId : nodes.keySet()) {
            if (searchId >= min && searchId <= max) {
                nodesList.add(searchId);
            }
        }
        return nodesList;
    }

    /**
     * Container needed for creation of backwards edges
     */
    private class ParentsEdgesContainer {
        private int destinationNode;
        private int weight;
        private LinkedList<Integer> originNodes;


        public ParentsEdgesContainer(int destinationNode) {
            this.destinationNode = destinationNode;
            weight = 0;
            originNodes = new LinkedList<>();
        }

        /**
         * Return the destination node from which starts all backwards edges.
         *
         * @return destination node from which starts all backwards edges
         */
        private int getDestinationNode() {
            return destinationNode;
        }

        /**
         * Return the weight of all the backwards edges.
         *
         * @return weight of all the backwards edges
         */
        public int getWeight() {
            return weight;
        }

        /**
         * Return the list of the origin nodes.
         *
         * @return list of the origin nodes
         */
        public LinkedList<Integer> getOriginNodes() {
            return originNodes;
        }

        /**
         * Add a new node to the list of originNode and update the weight of the backwards edges
         *
         * @param nodeSearchId the searchId of the node to insert
         */
        public void addOriginNode(int nodeSearchId) {
            originNodes.add(nodeSearchId);
            weight++;
        }
    }

    /**
     * Keep all the information of a table in the graph.
     */
    private class TableDefinition {

        // Variables declaration
        private int nElements;
        private int minSearchId;
        private int maxSearchId;
        private ArrayList<String> columns;

        /**
         * @param nElements   number of elements contained by the table
         * @param minSearchId min search id contained by the table
         * @param maxSearchId max search id contained by the table
         * @param columns     columns of the table
         */
        public TableDefinition(int nElements, int minSearchId, int maxSearchId, ArrayList<String> columns) {
            this.nElements = nElements;
            this.columns = columns;
            this.minSearchId = minSearchId;
            this.maxSearchId = maxSearchId;
        }

        /**
         * Return an ArrayList which store columns information.
         *
         * @return ArrayList of columns information
         */
        public ArrayList<String> getColumns() {
            return columns;
        }

        /**
         * Returns the size of the table.
         *
         * @return size of the table
         */
        public int getSize() {
            return nElements;
        }

        /**
         * Return the min search id of this table.
         *
         * @return min search id of this table
         */
        public int getMinSearchId() {
            return minSearchId;
        }

        /**
         * Return the max search id of this table.
         *
         * @return max search id of this table
         */
        public int getMaxSearchId() {
            return maxSearchId;
        }
    }

    /**
     * Container element that represent a graph node.
     */
    public class GraphNode {

        // Varibles declaration
        private int nodeWeight;
        private int searchId;
        private ArrayList<AdjElement> parentsList;

        public GraphNode(int searchId) {
            this.searchId = searchId;
            nodeWeight = 0;
            parentsList = new ArrayList<>();
        }

        /**
         * Adds a node that is a parent of the current node
         *
         * @param adjNodeIndex index of the parent node
         * @param edgeWeight   weight of the edge
         */
        public void addParentNode(int adjNodeIndex, int edgeWeight) {
            AdjElement aNode = new AdjElement(adjNodeIndex, edgeWeight);
            parentsList.add(aNode);
        }

        /**
         * Returns the parent list of the current node
         *
         * @return the parent list of the current node
         */
        public ArrayList<AdjElement> getParentsList() {
            if (parentsList == null) {
                return new ArrayList<>();
            }
            return parentsList;
        }

        /**
         * Return search id of the node.
         *
         * @return search id of the node
         */
        public int getSearchId() {
            return searchId;
        }

        /**
         * Return the weight of the node.
         *
         * @return the weight of the node
         */
        public int getNodeWeight() {
            return nodeWeight;
        }

        /**
         * Increment the weight of the node.
         */
        public void incrementNodeWeight() {
            nodeWeight++;
            if (nodeWeight < 0) {
                System.out.println("ERROR!! negative weight");
            }
        }

        /**
         * Class designed to hold the list of adjacent nodes to a node
         * adjElement contains the searchId of an adjacent node and the weight of the
         * corresponding edge
         */
        public class AdjElement {
            private int searchId;
            private int weight;

            public AdjElement(int searchId, int weight) {
                this.searchId = searchId;
                this.weight = weight;
            }

            public int getAdjNodeSearchId() {
                return searchId;
            }

            public int getEdgeWeight() {
                return weight;
            }
        }
    }

    /**
     * Implementation of a graph iterator which follows Dijkstra algorithm sequence.
     */
    public class GraphIterator implements Iterator, Comparable {
        private int currNode;
        private int nextDistance = 0;
        private int startNode;
        private int nTokensFound;
        private PriorityQueue<Integer> adj;
        private Int2ObjectOpenHashMap visitedNodes;
        public Int2ObjectOpenHashMap nodeDistances;
        private ArrayList<String> tokenKeywords;

        public GraphIterator(int startNode, ArrayList<String> tokenKeywords, int nTokensFound) throws LimitExceededException {
            visitedNodes = new Int2ObjectOpenHashMap();
            adj = new PriorityQueue<>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    int d1 = ((int[]) nodeDistances.get(o1))[0];
                    int d2 = ((int[]) nodeDistances.get(o2))[0];
                    if (d1 < d2) return -1;
                    else if (d1 == d2) return 0;
                    return 1;
                }
            });
            nodeDistances = new Int2ObjectOpenHashMap();
            this.startNode = startNode;
            this.nTokensFound = nTokensFound;
            this.tokenKeywords = tokenKeywords;
            currNode = -1;
            int temp[] = {0, -1, 0};
            nodeDistances.put(startNode, temp);
            adj.add(startNode);
        }

        /**
         * Return the number of tokens founds.
         *
         * @return number of tokens founds
         */
        public int getTokensWeight() {
            return nTokensFound;
        }

        /**
         * Add a list of token to this iterator if not already present
         *
         * @param tokens list of tokens
         */
        public void addTokens(ArrayList<String> tokens, int nTokensFound) {
            for (String token : tokens) {
                if (!tokenKeywords.contains(token)) {
                    tokenKeywords.add(token);
                }
            }
            this.nTokensFound += nTokensFound;
        }

        public Pair<IntArrayList, Pair<Integer, Double>> getWeightRootLeaf(int root, int leaf) {
            IntArrayList internalNodes = new IntArrayList();
            double weight = 0;
            int number = 0;
            int[] temp = ((int[]) visitedNodes.get(root));
            while (temp[0] != -1) {
                if (temp[0] != leaf) internalNodes.add(temp[0]);
                number++;
                weight += (Math.log(1 + temp[1])) / Math.log(2);
                temp = ((int[]) visitedNodes.get(temp[0]));
            }
            return new Pair<>(internalNodes, new Pair<>(number, weight));
        }

        /**
         * Returns the start node of the iterator.
         *
         * @return the start node of the iterator.
         */
        public int getStartNode() {
            return startNode;
        }

        /**
         * Returns the keyword associated to the iterator.
         *
         * @return the keyword associated to the iterator.
         */
        public ArrayList<String> getTokenKeywords() {
            return tokenKeywords;
        }

        /**
         * Returns the distance value of the node that the iterator will visit on
         * next iteration.
         *
         * @return the next-element-distance value
         */
        public int getNextElementDistance() {
            return nextDistance;
        }

        /**
         * Returns the searchId of the next node that the Dijkstra algorithm will visit,
         * Throws NoSuchElementException if the method has been called but
         * there's no next node to be visited.
         *
         * @return the searchId of the next node that the Dijkstra algorithm will visit
         */
        @Override
        public Object next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            int result = adj.poll();
            currNode = result;
            int temp[] = ((int[]) nodeDistances.remove(result));
            int distanceThisNode = temp[0];
            visitedNodes.put(result, new int[]{temp[1], temp[2]});
            ArrayList<GraphNode.AdjElement> adjElement = nodes.get(result).getParentsList();
            for (int i = 0; i < adjElement.size(); i++) {

                int value = adjElement.get(i).getAdjNodeSearchId();
                int distance = adjElement.get(i).getEdgeWeight();
                if (!visitedNodes.containsKey(value)) {
                    if (!nodeDistances.containsKey(value)) {
                        int[] valore = {distanceThisNode + distance, result, distance};
                        nodeDistances.put(value, valore);
                        adj.add(value);
                    } else {
                        int adiacente[] = ((int[]) nodeDistances.get(value));
                        if (adiacente[0] > distanceThisNode + distance) {
                            nodeDistances.remove(value);
                            int[] valore = {distanceThisNode + distance, result, distance};
                            nodeDistances.put(value, valore);
                            adj.remove(value);
                            adj.add(value);
                        }
                    }
                }
            }
            temp = ((int[]) nodeDistances.get(adj.peek()));
            if (temp != null) nextDistance = temp[2];
            else nextDistance = -1;
            return result;
        }

        @Override
        public boolean hasNext() {
            return adj.size() != 0;
        }

        @Override
        public int compareTo(Object o) {
            GraphIterator itemToCompare = (GraphIterator) o;
            return this.nextDistance - itemToCompare.getNextElementDistance();
        }

    }//{c} GraphIterator

    /**
     * Create or overwrite the output file of index.
     *
     * @return BufferWriter linked to the file
     * @throws IOException
     */
    public BufferedWriter createIndexFile() throws IOException {
        String outputFileAbsolutePaht;
        outputFileAbsolutePaht = AUX_FILE_PATH;
        File f = new File(outputFileAbsolutePaht);
        System.out.println("Check if previous index file exists on disk at path " + outputFileAbsolutePaht);
        if (f.exists()) {
            System.out.println("Previous existing index file deleted");
            f.delete();
        }
        System.out.println("New index file created at " + outputFileAbsolutePaht);
        BufferedWriter output = new BufferedWriter(new FileWriter(outputFileAbsolutePaht, true));
        return output;
    }

    /**
     * Write a line to the index file.
     *
     * @param tuple     tuple to write
     * @param nodeIndex index of the node in which is present the tuple
     * @param output    BufferWriter which write data to the file
     * @throws IOException
     */
    public void writeToIndexFile(ArrayList<String> tuple, int nodeIndex, BufferedWriter output) throws IOException {
        String LineToPrint = tuple.toString().toLowerCase().replaceAll("\\s", " ").replace("\n", " ").replace("\r", " ") + "\t" + nodeIndex + "\n";
        output.append(LineToPrint);
    }

    /**
     * Close and save the file created.
     *
     * @throws IOException
     */
    public void closeIndexFile() throws IOException {
        this.output.close();
    }
}