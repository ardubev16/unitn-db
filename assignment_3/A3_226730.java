import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class A3_226730 {
    static final int SIZE = 1000000;
    static final String user = "db_257";
    static final String password = "pass_257";
    static final String url = "jdbc:postgresql://sci-didattica.unitn.it/db_257";
    // static final String url = "jdbc:postgresql://localhost:5432/db_257";
    static final Random RNG = new Random();

    class RandomGenerator {
        private static final Random RNG = new Random();

        private static String string(int length) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                sb.append((char)(RNG.nextInt(26) + 'a'));
            }
            return sb.toString();
        }

        public static Iterator<String> stringList(int size, int length) {
            String[] strings = new String[size];
            for (int i = 0; i < size; i++) {
                strings[i] = RandomGenerator.string(length);
            }
            return Arrays.asList(strings).iterator();
        }

        public static ArrayList<Integer> idList(int size) {
            return RNG.ints(0, Integer.MAX_VALUE)
                .distinct()
                .limit(size)
                .boxed()
                .collect(Collectors.toCollection(ArrayList::new));
        }

        public static Iterator<Integer> intList(int size, int min, int max) {
            return RNG.ints(size, min, max).iterator();
        }

        public static Iterator<Double> doubleList(int size, float min,
                                                  float max) {
            return RNG.doubles(min, max).distinct().limit(size).iterator();
        }
    }

    public static void main(String[] args) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("ssl", "false");

        Connection connection = DriverManager.getConnection(url, props);
        Statement statement = connection.createStatement();
        ArrayList<Integer> professorIds = RandomGenerator.idList(SIZE);

        step01(statement);
        step02(statement);
        step03(connection, professorIds.iterator());
        step04(connection, professorIds);
        step05_09(statement, 5);
        step06(statement);
        step07(statement);
        step08(statement);
        step05_09(statement, 9);
        step10(statement);
        step11(statement);

        statement.close();
        connection.close();
    }

    private static void step01(Statement statement) throws SQLException {
        long startTime = System.nanoTime();

        String dropTables = "DROP TABLE IF EXISTS course; "
                            + "DROP TABLE IF EXISTS professor;";
        statement.executeUpdate(dropTables);

        long endTime = System.nanoTime();
        System.out.println("Step 1 needs " + (endTime - startTime) + " ns");
    }

    private static void step02(Statement statement) throws SQLException {
        long startTime = System.nanoTime();

        String professorTable = "CREATE TABLE professor ("
                                + "id INT PRIMARY KEY NOT NULL, "
                                + "name VARCHAR(50) NOT NULL, "
                                + "address VARCHAR(50) NOT NULL, "
                                + "age INT NOT NULL, "
                                + "department FLOAT NOT NULL );";
        String courseTable =
            "CREATE TABLE course ("
            + "cid VARCHAR(25) NOT NULL,"
            + "cname VARCHAR(50) NOT NULL,"
            + "credits VARCHAR(30) NOT NULL,"
            + "teacher INT NOT NULL,"
            + "FOREIGN KEY (teacher) REFERENCES professor (id) );";
        String query = professorTable + courseTable;
        statement.executeUpdate(query);

        long endTime = System.nanoTime();
        System.out.println("Step 2 needs " + (endTime - startTime) + " ns");
    }

    private static void step03(Connection connection, Iterator<Integer> ids)
        throws SQLException {
        long startTime = System.nanoTime();

        Iterator<String> names = RandomGenerator.stringList(SIZE, 50);
        Iterator<String> addresses = RandomGenerator.stringList(SIZE, 50);
        Iterator<Integer> ages = RandomGenerator.intList(SIZE, 30, 70);
        Iterator<Double> departments =
            RandomGenerator.doubleList(SIZE - 1, 0, 1939);

        String query = "INSERT INTO professor VALUES(?,?,?,?,?)";
        PreparedStatement statement = connection.prepareStatement(query);
        for (int i = 0; i < SIZE; i++) {
            statement.setInt(1, ids.next());
            statement.setString(2, names.next());
            statement.setString(3, addresses.next());
            statement.setInt(4, ages.next());
            if (i == SIZE - 1)
                statement.setFloat(5, 1940);
            else
                statement.setFloat(5, departments.next().floatValue());

            statement.addBatch();
        }
        statement.executeBatch();

        long endTime = System.nanoTime();
        System.out.println("Step 3 needs " + (endTime - startTime) + " ns");
    }

    private static void step04(Connection connection,
                               ArrayList<Integer> teachers)
        throws SQLException {
        long startTime = System.nanoTime();

        Iterator<String> cids = RandomGenerator.idList(SIZE)
                                    .stream()
                                    .map(String::valueOf)
                                    .collect(Collectors.toList())
                                    .iterator();
        Iterator<String> cnames = RandomGenerator.stringList(SIZE, 50);
        Iterator<String> credits = RandomGenerator.stringList(SIZE, 30);

        String query = "INSERT INTO course VALUES (?,?,?,?)";
        PreparedStatement statement = connection.prepareStatement(query);

        for (int i = 0; i < SIZE; i++) {
            statement.setString(1, cids.next());
            statement.setString(2, cnames.next());
            statement.setString(3, credits.next());
            statement.setInt(4, teachers.get(RNG.nextInt(SIZE)));

            statement.addBatch();
        }
        statement.executeBatch();

        long endTime = System.nanoTime();
        System.out.println("Step 4 needs " + (endTime - startTime) + " ns");
    }

    private static void step05_09(Statement statement, int n)
        throws SQLException {
        long startTime = System.nanoTime();

        String query = "SELECT id FROM professor";
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
            System.err.println(resultSet.getInt("id"));
        }

        long endTime = System.nanoTime();
        System.out.println("Step " + n + " needs " + (endTime - startTime) +
                           " ns");
    }

    private static void step06(Statement statement) throws SQLException {
        long startTime = System.nanoTime();

        String query = "UPDATE professor "
                       + "SET department = 1973 "
                       + "WHERE department = 1940;";
        statement.executeUpdate(query);

        long endTime = System.nanoTime();
        System.out.println("Step 6 needs " + (endTime - startTime) + " ns");
    }

    private static void step07(Statement statement) throws SQLException {
        long startTime = System.nanoTime();

        String query =
            "SELECT id, address FROM professor WHERE department = 1973;";
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
            System.err.println(resultSet.getInt("id") + "," +
                               resultSet.getString("address"));
        }

        long endTime = System.nanoTime();
        System.out.println("Step 7 needs " + (endTime - startTime) + " ns");
    }

    private static void step08(Statement statement) throws SQLException {
        long startTime = System.nanoTime();

        String query = "CREATE INDEX b_tree ON professor (department);";
        statement.executeUpdate(query);

        long endTime = System.nanoTime();
        System.out.println("Step 8 needs " + (endTime - startTime) + " ns");
    }

    private static void step10(Statement statement) throws SQLException {
        long startTime = System.nanoTime();

        String query = "UPDATE professor "
                       + "SET department = 1974 "
                       + "WHERE department = 1973;";
        statement.executeUpdate(query);

        long endTime = System.nanoTime();
        System.out.println("Step 10 needs " + (endTime - startTime) + " ns");
    }

    private static void step11(Statement stmt) throws SQLException {
        long startTime = System.nanoTime();

        String query =
            "SELECT id, address FROM professor WHERE department = 1974;";
        ResultSet resultSet = stmt.executeQuery(query);
        while (resultSet.next()) {
            System.err.println(resultSet.getInt("id") + "," +
                               resultSet.getString("address"));
        }

        long endTime = System.nanoTime();
        System.out.println("Step 11 needs " + (endTime - startTime) + " ns");
    }
}
