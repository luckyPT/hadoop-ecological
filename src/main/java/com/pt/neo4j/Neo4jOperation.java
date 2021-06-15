package com.pt.neo4j;

import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jOperation {
    static Session session = null;

    public static void insertNode(String label, Map<String, Object> properties) {
        String name = "entity";
        StatementResult result = session.run("CREATE (" + name + ":" + node2String(label, properties) + ") return " + name);
        System.out.println(result.consume());
    }

    public static void deleteNode(String label, Map<String, Object> properties) {
        StatementResult result = session.run("MATCH (a:" + node2String(label, properties) + ") DELETE a");
        System.out.println(result.consume());
    }


    public static List<Record> findAllNode(String label, List<String> returnProperties) {
        String name = "entity";
        StatementResult result = session.run("MATCH (" + name + ":" + label + ")" +
                " RETURN " + properties2String(name, returnProperties) + ", ID(" + name + ")");
        return result.list();
    }

    public static List<Record> findNodeById(int id, List<String> returnProperties) {
        String name = "entity";
        return session.run("MATCH (" + name + ") WHERE ID(" + name + ") = " + id +
                " RETURN " + properties2String(name, returnProperties) + ", ID(" + name + ")").list();
    }

    public static List<Record> findNodeByProperties(String label, Map<String, Object> conditions, List<String> returnProperties) {
        String name = "entity";
        StatementResult result = session.run("MATCH (" + name + ":" + node2String(label, conditions) + ")" +
                " RETURN " + properties2String(name, returnProperties) + ", ID(" + name + ")");
        return result.list();
    }

    /**
     * 创建两个新节点，并创建关系
     *
     * @param label1             节点1的类型
     * @param properties1        节点1的属性
     * @param label2             节点2的类型
     * @param properties2        节点2的属性
     * @param relation           关系类型
     * @param relationProperties 关系属性
     */
    public static void createNodesWithRelation(String label1, Map<String, Object> properties1,
                                               String label2, Map<String, Object> properties2,
                                               String relation, Map<String, Object> relationProperties) {
        String name1 = "node1";
        String name2 = "node2";
        StatementResult result = session.run("CREATE(" + name1 + ":" + node2String(label1, properties1) + ")" +
                "-[" + relation + ":" + node2String(relation, relationProperties) + "]->" +
                "(" + name2 + ":" + node2String(label2, properties2) + ")");
        System.out.println(result.consume());
    }

    /**
     * 为两个已经存在的节点创建关系
     *
     * @param label1             节点1的类型
     * @param properties1        节点1的属性
     * @param label2             节点2的类型
     * @param properties2        节点2的属性
     * @param relation           关系类型
     * @param relationProperties 关系属性
     */
    public static void createRelation(String label1, Map<String, Object> properties1,
                                      String label2, Map<String, Object> properties2,
                                      String relation, Map<String, Object> relationProperties) {
        String name1 = "node1";
        String name2 = "node2";
        StatementResult result = session.run("MATCH (" + name1 + ":" + node2String(label1, properties1) + "),(" +
                name2 + ":" + node2String(label2, properties2) + ")" +
                "CREATE (" + name1 + ")-[" + relation + ":" + node2String(relation, relationProperties) + "]->(" + name2 + ")");
    }

    public static void deleteRelation(String label1, Map<String, Object> properties1,
                                      String label2, Map<String, Object> properties2,
                                      String relation, Map<String, Object> relationProperties) {
        String name1 = "node1";
        String name2 = "node2";
        StatementResult result = session.run("Match(" + name1 + ":" + node2String(label1, properties1) +
                ")-[" + relation + ":" + node2String(relation, relationProperties) + "]-(" + name2 + ":" +
                node2String(label2, properties2) + ") DELETE " + relation);
        System.out.println(result.consume());
    }

    public static List<Record> getRelationNodesLevel1(String label, Map<String, Object> properties1,
                                                      String resultLabel,
                                                      String relation,
                                                      Map<String, Object> relationProperties,
                                                      List<String> returnProperties,
                                                      List<String> returnRelationProperties) {
        String name1 = "node1";
        String name2 = "node2";
        StatementResult result = session.run(
                "Match(" + name1 + ":" + node2String(label, properties1) + ")-[" + relation + ":" +
                        node2String(relation, relationProperties) + "]-(" + name2 + ":" + resultLabel +
                        ") RETURN " + properties2String(name2, returnProperties) +
                        ", " + properties2String(relation, returnRelationProperties)
        );
        return result.list();
    }

    public static void truncateDb() {
        StatementResult result = session.run("match (n) detach delete n");
        System.out.println(result.consume());
    }

    private static String node2String(String label, Map<String, Object> properties) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(entry.getKey()).append(":\"").append(entry.getValue()).append("\"");
        }
        return label + "{" + sb.toString() + "}";
    }

    /**
     * @param nodeName         标签类型
     * @param returnProperties 返回的属性
     * @return
     */
    private static String properties2String(String nodeName, List<String> returnProperties) {
        StringBuilder sb = new StringBuilder();
        for (String s : returnProperties) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(nodeName).append(".").append(s);
        }
        return sb.toString();
    }


    public static void main(String[] args) {
        session = GraphDatabase.driver("bolt://10.221.97.51:7687",
                AuthTokens.basic("neo4j", "NEO4J")).session();
        truncateDb();
        List<int[]> testData = new ArrayList<int[]>() {{
            add(new int[]{1, 2, 4});
            add(new int[]{1, 3, 5});
            add(new int[]{1, 4, 6});
            add(new int[]{3, 4, 8});
            add(new int[]{3, 5, 9});
        }};
        String label = "Item";
        String id = "item_id";
        String relationName = "co_occurrence";
        String relationProperty = "count";
        for (int[] data : testData) {
            if (findNodeByProperties(label, new HashMap<String, Object>() {{
                put(id, data[0]);
            }}, new ArrayList<String>() {{
                add(id);
            }}).size() == 0) {
                insertNode(label, new HashMap<String, Object>() {{
                    put(id, data[0]);
                }});
            }

            if (findNodeByProperties(label, new HashMap<String, Object>() {{
                put(id, data[1]);
            }}, new ArrayList<String>() {{
                add(id);
            }}).size() == 0) {
                insertNode(label, new HashMap<String, Object>() {{
                    put(id, data[1]);
                }});
            }


            createRelation(label, new HashMap<String, Object>() {{
                put(id, data[0]);
            }}, label, new HashMap<String, Object>() {{
                put(id, data[1]);
            }}, relationName, new HashMap<String, Object>() {{
                put(relationProperty, data[2]);
            }});
        }

        System.out.println(findAllNode(label, new ArrayList<String>() {{
            add(id);
        }}));

        System.out.println(findNodeById(1024, new ArrayList<String>() {{
            add(id);
        }}));

        System.out.println(findNodeByProperties(label, new HashMap<String, Object>() {{
            put(id, 2);
        }}, new ArrayList<String>() {{
            add(id);
        }}));

        deleteRelation(label, new HashMap<String, Object>() {{
                    put(id, 1);
                }}, label, new HashMap<String, Object>() {{
                    put(id, 2);
                }},
                relationName, new HashMap<String, Object>() {{

                }});

        System.out.println(getRelationNodesLevel1(label, new HashMap<String, Object>() {{
            put(id, 3);
        }}, label, relationName, new HashMap<String, Object>() {{

        }}, new ArrayList<String>() {{
            add(id);
        }}, new ArrayList<String>() {{
            add(relationProperty);
        }}));
    }
}
