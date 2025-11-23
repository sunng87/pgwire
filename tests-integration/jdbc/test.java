///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.postgresql:postgresql:42.7.8

import java.sql.*;

public class test {
        public static void main(String[] args) throws Exception {
                String url = "jdbc:postgresql://127.0.0.1:5432/localdb";
                String user = "postgres";
                String password = "pencil";

                try (Connection conn = DriverManager.getConnection(url, user, password)) {
                        System.out.println("Connected to PostgreSQL server!");

                        // Test INSERT
                        try (Statement stmt = conn.createStatement()) {
                                int result = stmt.executeUpdate("INSERT INTO testtable VALUES (1)");
                                System.out.println("INSERT result: " + result);
                        }

                        // Test SELECT all
                        try (Statement stmt = conn.createStatement();
                                        ResultSet rs = stmt.executeQuery("SELECT * FROM testtable")) {
                                System.out.println("SELECT all results:");
                                while (rs.next()) {
                                        System.out.println("  Row: " + rs.getInt(1));
                                }
                        }

                        // Test SELECT with parameter
                        try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM testtable WHERE id = ?")) {
                                pstmt.setInt(1, 1);
                                try (ResultSet rs = pstmt.executeQuery()) {
                                        System.out.println("SELECT with parameter results:");
                                        while (rs.next()) {
                                                System.out.println("  Row: " + rs.getInt(1));
                                        }
                                }
                        }

                        // Test the mock server's SELECT query
                        try (Statement stmt = conn.createStatement();
                                        ResultSet rs = stmt.executeQuery("SELECT * FROM users")) {
                                System.out.println("Mock server SELECT results:");
                                ResultSetMetaData metaData = rs.getMetaData();
                                int columnCount = metaData.getColumnCount();

                                while (rs.next()) {
                                        for (int i = 1; i <= columnCount; i++) {
                                                String columnName = metaData.getColumnName(i);
                                                Object value = rs.getObject(i);
                                                System.out.println("  " + columnName + ": " + value);
                                        }
                                        System.out.println();
                                }
                        }

                        // Test prepared statement with 10 different data types
                        try (PreparedStatement pstmt = conn.prepareStatement(
                                        "SELECT * FROM test_table WHERE " +
                                                        "int_field = ? AND " +
                                                        "long_field = ? AND " +
                                                        "float_field = ? AND " +
                                                        "double_field = ? AND " +
                                                        "string_field = ? AND " +
                                                        "boolean_field = ? AND " +
                                                        "timestamp_field = ? AND " +
                                                        "date_field = ? AND " +
                                                        "decimal_field = ? AND " +
                                                        "bytes_field = ?")) {

                                // Set parameters with different data types
                                pstmt.setInt(1, 42); // int
                                pstmt.setLong(2, 123456789012345L); // long
                                pstmt.setFloat(3, 3.14f); // float
                                pstmt.setDouble(4, 2.718281828); // double
                                pstmt.setString(5, "test_string"); // string
                                pstmt.setBoolean(6, true); // boolean
                                pstmt.setTimestamp(7, Timestamp.valueOf("2023-12-25 10:30:45")); // timestamp
                                pstmt.setDate(8, Date.valueOf("2023-12-25")); // date
                                pstmt.setBigDecimal(9, new java.math.BigDecimal("123.456")); // decimal
                                pstmt.setBytes(10, "binary_data".getBytes()); // bytes

                                try (ResultSet rs = pstmt.executeQuery()) {
                                        System.out.println("Prepared statement with 10 data types results:");
                                        ResultSetMetaData metaData = rs.getMetaData();
                                        int columnCount = metaData.getColumnCount();

                                        while (rs.next()) {
                                                for (int i = 1; i <= columnCount; i++) {
                                                        String columnName = metaData.getColumnName(i);
                                                        Object value = rs.getObject(i);
                                                        System.out.println("  " + columnName + ": " + value);
                                                }
                                                System.out.println();
                                        }
                                }
                        }
                }
        }
}
