import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import javafx.util.Pair;
import javax.naming.LimitExceededException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.URL;
import java.util.*;

/**
 * Implementation of the algorithm.
 */

public class Banks {
    // Constants
    private final double LAMBDA = 0.2;
    private static int ntest = 1;
    static final int MAX_RESULTS_NUMBER = 10;
    private int MAX_OUTPUTHEAPSIZE;
    // Variable declarations
    private PriorityQueue<ResultTree> outputHeap;
    private String[] keywords;
    private Int2ObjectOpenHashMap visitedNodes;
    private Graph g;
    private Int2DoubleOpenHashMap normalizedNodeWeights;
    private ObjectArrayList<ResultTree> returnedElementsList;
    private PrintWriter output;
    private Int2ObjectOpenHashMap iteratorsMap;
    private double maxWeight;
    private long startTime;
    private GUI gui;
    private ArrayList<ResultTree> heapToReturn;

    public Banks(Graph g, String[] keywords, GUI gui, int heapSize) throws FileNotFoundException {
        heapToReturn = new ArrayList<>();
        this.g = g;
        this.keywords = keywords;
        this.gui = gui;
        MAX_OUTPUTHEAPSIZE = heapSize;
        startTime = System.currentTimeMillis();
        iteratorsMap = new Int2ObjectOpenHashMap();
        returnedElementsList = new ObjectArrayList();
        outputHeap = new PriorityQueue();
        normalizedNodeWeights = new Int2DoubleOpenHashMap();
        String separator = File.separator;
        output = new PrintWriter(new File("." + separator + "output_files" + separator + ntest + ".txt"));
        ntest++;
        for (int i = 0; i < keywords.length; i++) {
            keywords[i] = keywords[i].toLowerCase();
        }
        //save the normalized weight of each node in the graph
        maxWeight = g.getMaxNodeWeight();
        for (Object o : g.nodes.values()) {
            Graph.GraphNode n = (Graph.GraphNode) o;
            int tWeight = n.getNodeWeight();
            normalizedNodeWeights.put(n.getSearchId(), ((double) tWeight) / maxWeight);
        }
    }


    ArrayList<ResultTree> execute() throws FileNotFoundException, LimitExceededException {
        try {
            int treeReturned = 0;
            int numberOfSearchTerms = keywords.length;
            PriorityQueue<Graph.GraphIterator> iterQueue = g.getIterators(keywords);
            String kw = "";
            ArrayList<LinkedList<Integer>> product;
            for (String s : keywords) {
                kw += s + " ,";
            }
            kw = kw.substring(0, kw.length() - 2);
            System.out.println("Looking for keywords: " + kw);
            output.println("Looking for keywords: " + kw);
            if (keywords.length == 1) {
                int added = 0;
                Iterator current = iterQueue.iterator();
                while (current.hasNext()) {
                    Graph.GraphIterator g = (Graph.GraphIterator) current.next();
                    gui.updateUsedMemoryStats();
                    gui.checkTime(startTime);
                    gui.checkMemory();
                    ResultTree tree = new ResultTree(g.getStartNode());
                    added++;
                    addTreeToHeap(tree);
                }
            } else {
                System.out.printf("Currently there are %s iterators in the priority queue\n", iterQueue.size());
                Iterator iter = iterQueue.iterator();
                ArrayList<Integer> iteratorNames = new ArrayList<>();
                //number of leaves = #iterators
                int n = iterQueue.size();
                while (iter.hasNext()) {
                    Graph.GraphIterator gi = (Graph.GraphIterator) iter.next();
                    gui.updateUsedMemoryStats();
                    gui.checkTime(startTime);
                    gui.checkMemory();
                    // save the search id of the start nodes of the iterators
                    int node = gi.getStartNode();
                    int numberOfKey;
                    double weight;
                    iteratorNames.add(node);
                    numberOfKey = gi.getTokensWeight();
                    if (numberOfKey > 1) {
                        weight = normalizedNodeWeights.get(node);
                        weight *= numberOfKey;
                        normalizedNodeWeights.replace(node, weight);
                    }
                    numberOfKey = gi.getTokenKeywords().size();
                    if (numberOfKey == keywords.length) {
                        ResultTree tree = new ResultTree(node);
                        addTreeToHeap(tree);
                    }
                    //populate the iterator hash map
                    iteratorsMap.put(gi.getStartNode(), gi);
                }

                visitedNodes = new Int2ObjectOpenHashMap();

                while (!iterQueue.isEmpty() && treeReturned < MAX_RESULTS_NUMBER) {
                    Graph.GraphIterator tempIterator = iterQueue.poll();
                    /*
                    v is the currently visited node
                    itListsOrganizedByKw is a list associated to each node in visitedNodes.
                    itListsOrganizedByKW = iterator lists organized by keyword is an hashmap with
                    key = a search term, with value an arraylist containing all the startNodes of
                    the iterators that have visited that node v.
                    */
                    int v = (int) tempIterator.next();
                    gui.updateUsedMemoryStats();
                    gui.checkTime(startTime);
                    gui.checkMemory();
                    //put the iterator back in the priority queue
                    if (tempIterator.hasNext()) {
                        iterQueue.add(tempIterator);
                    }
                    //if v was not visited yet
                    if (!visitedNodes.containsKey(v)) {

                        // Create the hash map itListsOrganizedByKw that will hold all the
                        // lists of iterators associated to each search term in the query.
                        // Create all the lists of iterators associated to each search term in the query
                        // (those terms are already splitted and contained in keywords[]).
                        HashMap<String, ArrayList<Integer>> itListsOrganizedByKw = new HashMap<>();
                        for (int i = 0; i < numberOfSearchTerms; i++) {
                            ArrayList<Integer> L_i = new ArrayList<>();
                            itListsOrganizedByKw.put(keywords[i], L_i);
                        }
                        //Add the EMPTY lists and the "listholder" in the visitedNodes list
                        //in the place corresponding to node v.
                        visitedNodes.put(v, itListsOrganizedByKw);

                    }
                    ArrayList<String> temp;
                    ArrayList<String> keyword = tempIterator.getTokenKeywords();
                    ArrayList<Integer> indexOrigin = new ArrayList<>();
                    // get the index of the keyword of this iterator
                    for (int j = 0; j < keyword.size(); j++)
                        for (int i = 0; i < keywords.length; i++) {
                            {
                                if (keyword.get(j).equals(keywords[i])) {
                                    indexOrigin.add(j, i);
                                    break;
                                }
                            }
                        }
                    product = crossProduct(tempIterator.getStartNode(), v, indexOrigin);
                    for (int j = 0; j < product.size(); j++) {
                        ResultTree tree = new ResultTree(v, product.get(j));
                        if (tree.getTotalNodesNumber() < 7) {
                            if (tree.getLeavesNumber() > 1 || (tree.getLeavesNumber() == 1 && tree.getTotalNodesNumber() == 2)) {
                                if (isOutputHeapFull()) {
                                    //output top result from outputHeap
                                    ResultTree treeToOutput = outputHeap.poll();
                                    returnedElementsList.add(treeToOutput);
                                    System.out.println(treeToOutput);
                                    heapToReturn.add(treeReturned, treeToOutput);
                                    output.println(treeToOutput.toString());
                                    treeReturned++;
                                    if (MAX_RESULTS_NUMBER == treeReturned) {
                                        System.out.println("Have reached maximum number of results to output, exit.");
                                        return heapToReturn;
                                    }
                                }
                                addTreeToHeap(tree);
                            }
                        }
                    }
                    //Add the current iterator startNode number to the proper list
                    //associated to the iterator corresponding keyword (AKA search term).
                    HashMap<String, ArrayList<Integer>> itListsOrganizedByKw = (HashMap<String, ArrayList<Integer>>) visitedNodes.get(v);
                    temp = tempIterator.getTokenKeywords();
                    for (int k = 0; k < temp.size(); k++) {
                        ArrayList<Integer> L_i = itListsOrganizedByKw.get(temp.get(k).toLowerCase());
                        L_i.add(tempIterator.getStartNode());
                    }
                }// while there are more iterators or there are more results to be returned
            }
            System.out.println("Iterator queue emptied");
            //if I haven't returned enough trees then return the prefixed number of answers
            System.out.println("Output heap size: " + outputHeap.size());
            while (treeReturned < MAX_RESULTS_NUMBER && outputHeap.size() != 0) {
                ResultTree treeToOutput = outputHeap.poll();
                System.out.println(treeToOutput);
                heapToReturn.add(treeReturned, treeToOutput);
                output.println(treeToOutput.toString());
                treeReturned++;
            }
            if (treeReturned == 0) {
                System.out.println("No tree found for current query");
            }
        } catch (Throwable e) {
            System.out.println("!!! ERROR FOUND WHILE EXECUTING BANKS ALGORITHM !!!");
            System.out.println(e.getMessage());
            output.println(e.getMessage());
        } finally {
            output.close();
            return heapToReturn;
        }
    }//[m] execute

    /**
     * Performs the checks before adding a new tree to the result heap.
     * if I found a tree with the same leaves and a different root,
     * that has a lower weight (worse) than the current one, replace
     * the one in the heap with the current one
     *
     * @param treeToAdd New tree to add.
     */
    public void addTreeToHeap(ResultTree treeToAdd) {
        for (ResultTree tempTree : returnedElementsList) {
            if (tempTree.equals(treeToAdd)) {
                return;
            }
        }
        for (ResultTree tempTree : outputHeap) {
            if (tempTree.equals(treeToAdd)) {
                if (tempTree.getWeight() > treeToAdd.getWeight()) {
                    return;
                } else {
                    outputHeap.remove(tempTree);
                    break;
                }
            }
        }
        outputHeap.add(treeToAdd);
    }


    /**
     * The set of tuples that compose the cross product
     *
     * @param origin             origin node of this iterator
     * @param current            node just visited
     * @param indexOriginKeyword index of the keyword associated to this iterator
     * @return The set of tuples that compose the cross product.
     */

    public ArrayList<LinkedList<Integer>> crossProduct(int origin, int current, ArrayList<Integer> indexOriginKeyword) {
        ArrayList<LinkedList<Integer>> result = new ArrayList<>();
        LinkedList<Integer> temp = new LinkedList<>();
        temp.add(origin);
        HashMap<String, ArrayList<Integer>> list = (HashMap<String, ArrayList<Integer>>) visitedNodes.get(current);
        int total = 1;
        int length;
        int value;
        // get the number of the tuple
        for (int i = 0; i < keywords.length; i++) {
            if (!indexOriginKeyword.contains(i)) {
                total *= list.get(keywords[i]).size();
            }
        }
        if (total == 0) {
            return result;
        }
        // add to each tuple the list with the origin node
        for (int i = 0; i < total; i++) {
            result.add(i, (LinkedList<Integer>) temp.clone());
        }
        // for each list associated to the current node
        for (int i = 0; i < keywords.length; i++) {
            if (!indexOriginKeyword.contains(i)) {
                length = list.get(keywords[i]).size();
                // for each element in this list L_i
                for (int j = 0; j < length; j++) {
                    // get the value of the element
                    value = list.get(keywords[i]).get(j);
                    for (int k = 0; k < total / length; k++) {
                        temp = result.get(j + k * length);
                        if (!temp.contains(value)) temp.add(value);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Returns true if the outputHeap has more than MAX_OUTPUTHEAPSIZE elements, false otherwise
     *
     * @return true if the outputHeap has more than MAX_OUTPUTHEAPSIZE elements, false otherwise
     */
    private boolean isOutputHeapFull() {
        return outputHeap.size() >= MAX_OUTPUTHEAPSIZE;
    }

    class ResultTree implements Comparable {
        private int root;
        private ArrayList<Integer> leaves;
        private double overallNodeScore;
        private double overallEdgeScore;
        private double overallTreeRelevanceScore;
        private double overallTreeRelevanceScoreMult;
        private IntArrayList internalNodes;
        int leavesNumber;
        int totalNode;

        public ResultTree(int root) {
            this.root = root;
            leaves = new ArrayList<>();
            internalNodes = new IntArrayList();
            overallNodeScore = normalizedNodeWeights.get(root);
            overallEdgeScore = 1;
            overallTreeRelevanceScore = (1 - LAMBDA) * overallEdgeScore + LAMBDA * overallNodeScore;
            overallTreeRelevanceScoreMult = overallEdgeScore * Math.pow(overallNodeScore, LAMBDA);
            totalNode = 1;
        }

        public double getWeight() {
            return overallTreeRelevanceScore;
        }

        public ResultTree(int root, LinkedList<Integer> nodesList) {
            this.root = root;
            leavesNumber = 0;
            leaves = new ArrayList<>();
            internalNodes = new IntArrayList();
            // Add elements to the leaves list
            for (int i = 0; i < nodesList.size(); i++) {
                if (nodesList.get(i) != root) {
                    leaves.add(nodesList.get(i));
                    leavesNumber++;
                }
            }
            // Compute overall node weight
            overallNodeScore = normalizedNodeWeights.get(root);
            int count = 1;
            for (int n : leaves) {
                overallNodeScore += normalizedNodeWeights.get(n);
                count++;
            }
            overallNodeScore /= count;

            // Compute overall Edge weight
            double totEdgeWeight = 0;

            //for each leaf look for the respective iterator path covered to reach it
            for (int leaf : leaves) {
                Graph.GraphIterator tempGIterator = (Graph.GraphIterator) iteratorsMap.get(leaf);
                Pair<IntArrayList, Pair<Integer, Double>> result = tempGIterator.getWeightRootLeaf(root, leaf);
                IntArrayList temp = result.getKey();
                for (int i = 0; i < temp.size(); i++) internalNodes.add(temp.get(i));
                totEdgeWeight += result.getValue().getValue();
                totalNode += result.getValue().getKey();
            }
            totalNode++;
            overallEdgeScore = 1 / (1 + totEdgeWeight);
            overallTreeRelevanceScore = (1 - LAMBDA) * overallEdgeScore + LAMBDA * overallNodeScore;
            overallTreeRelevanceScoreMult = overallEdgeScore * Math.pow(overallNodeScore, LAMBDA);
        }//[m] ResultTree

        public int getTotalNodesNumber() {
            return totalNode;
        }

        /**
         * Returns the number of leaves in the tree.
         *
         * @return The number of leaves in the tree.
         */
        public int getLeavesNumber() {
            return leavesNumber;
        }

        @Override
        public int compareTo(Object o) {
            ResultTree otherTree = (ResultTree) o;
            //return (int) ((otherTree.overallTreeRelevanceScore - this.overallTreeRelevanceScore) * 1000000);
            return Double.compare(otherTree.overallTreeRelevanceScore, this.overallTreeRelevanceScore);

        }

        @Override
        public String toString() {
            String retString = "";
            try {
                retString = "\n----------------------\n" +
                        "TREE: \n" +
                        "Weight addition: " + overallTreeRelevanceScore + "\n" +
                        "Weight multiplication: " + overallTreeRelevanceScoreMult + "\n" +
                        "Node Weight: " + overallNodeScore + "\n" +
                        "Edge Weight: " + overallEdgeScore + "\n" +
                        "Total nodes number: " + getTotalNodesNumber() + "\n" +
                        "Root: " + root + " " + findTupleOfNode(root) + "\n" +
                        "Internal nodes: [ ";
                for (int j = 0; j < internalNodes.size(); j++)
                    retString += internalNodes.get(j) + " ";
                retString += "]\nLeaves: \n";
                for (int i = 0; i < leaves.size(); i++) {
                    int leaf = leaves.get(i);
                    retString += findTupleOfNode(leaf) + " " + leaf + "\n";
                }
                retString += "\n----------------------\n";
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            return retString;
        }

        /**
         * Returns the tuple associated to the node with given RID
         *
         * @param RID the RID of the node to find the tuple of
         * @return the tuple associated to the node with given RID
         * @throws FileNotFoundException
         */
        public String findTupleOfNode(int RID) throws FileNotFoundException {
            String outputFileAbsolutePaht = Graph.AUX_FILE_PATH;
            Scanner in = new Scanner(new FileReader(outputFileAbsolutePaht));
            while (in.hasNext()) {
                String line = "";
                while (!line.contains("\t") && in.hasNext()) {
                    line += in.nextLine();
                }
                String tTuple = line.split("\t")[0];
                String tRID = line.split("\t")[1];
                if (tRID.compareTo(String.valueOf(RID)) == 0) {
                    return tTuple;
                }
            }
            return "";
        }

        public int getRoot() {
            return root;
        }

        public ArrayList<Integer> getLeaves() {
            return leaves;
        }

        /**
         * Compares two ResultTree by looking ONLY at their leaves.
         *
         * @param o the other tree to be compared.
         * @return true if the two trees have the same leaves and the same root, false otherwise.
         */
        @Override
        public boolean equals(Object o) {
            ResultTree tree = (ResultTree) o;
            if (tree.getTotalNodesNumber() == 2 && this.getTotalNodesNumber() == 2 &&
                    tree.getRoot() == this.getLeaves().get(0) && this.getRoot() == tree.getLeaves().get(0)) {
                return true;
            }
            if (tree.getRoot() != this.getRoot() || this.getLeavesNumber() != tree.getLeavesNumber() ||
                    this.getTotalNodesNumber() != tree.getTotalNodesNumber()) {
                return false;
            }
            for (int i = 0; i < tree.getLeavesNumber(); i++) {
                if (!this.getLeaves().contains(tree.getLeaves().get(i))) {
                    return false;
                }
            }
            return true;
        }
    }//[c] ResultTree
}

