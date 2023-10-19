package ru.ac.uniyar.databasescourse;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;

public class DatabaseExample {
    private static final String URL = String.format("jdbc:mariadb://%s", System.getenv("MARIADB_HOST"));
    private static final String user = System.getenv("MARIADB_USER");
    private static final String password = System.getenv("MARIADB_PASSWORD");

    public static void main(String[] args) {

        System.out.println("The work has started");

        try (Connection conn = createConnection()) {
            try (Statement smt = conn.createStatement()) {
                smt.executeQuery("USE IrinaTolmachevaDB");
                try {
                    {
                        ResultSet rs = dropTable(conn).executeQuery();
                        while (rs.next()) {
                            System.out.printf("DATABASE %s\n", rs.getString(0));
                        }
                    }
                    {
                        ResultSet rs = createTable(conn).executeQuery();
                        while (rs.next()) {
                            System.out.printf("DATABASE %s\n", rs.getString(0));
                        }
                    }
                    ArrayList<PreparedStatement> inserts = insertSolutions(conn);
                    for(PreparedStatement insert: inserts){
                        ResultSet rs = insert.executeQuery();
                        while (rs.next()) {
                            System.out.printf("Student added: %s %s\n", rs.getString(1),rs.getString(2));
                        }
                    }
                    //{
                     //   ResultSet rs = showTable(conn).executeQuery();
                      //  while (rs.next()) {
                       //     System.out.printf("Student :%s %s %s %s %s %s %s %s %s\n", rs.getString(1), rs.getString(2),rs.getString(3),  rs.getString(4), rs.getString(5),rs.getString(6),  rs.getString(7), rs.getString(8),rs.getString(9));
                        //}
                    //}
                    ResultSet rs = selectMaxScore(conn).executeQuery();
                    while (rs.next()) {
                        System.out.printf("%s:  %s %s %s\n", rs.getString(1),
                                rs.getString(2), rs.getString(3), rs.getString(4));
                    }
                    ResultSet rs1 = selectMinScore(conn).executeQuery();
                    while (rs1.next()) {
                        System.out.printf("%s: %s %s %s\n", rs1.getString(1),
                                rs1.getString(2), rs1.getString(3), rs1.getString(4));
                    }
                    ResultSet rs2 = topTeachers(conn).executeQuery();
                    while (rs2.next()) {
                        System.out.printf("%s: %s %s\n", rs2.getString(1),
                                rs2.getString(2), rs2.getString(3));
                    }
                    {
                        ResultSet rs3 = dnoTeachers(conn).executeQuery();
                        while (rs3.next()) {
                            System.out.printf("%s: %s %s\n", rs3.getString(1),
                                    rs3.getString(2), rs3.getString(3));
                        }
                    }
                    {
                        ResultSet rs3 = selectAssigStudBuTeach(conn).executeQuery();
                        while (rs3.next()) {
                            System.out.printf("%s: %s %s %s %s\n", rs3.getString(1),
                                    rs3.getString(2), rs3.getString(3),
                                    rs3.getString(4), rs3.getString(5));
                        }
                    }
                    {
                        ResultSet rs3 = selectMany(conn).executeQuery();
                        System.out.printf("\n\n\n\n");
                        while (rs3.next()) {
                            System.out.printf("%s %s: %s %s\n", rs3.getString(1),
                                    rs3.getString(2), rs3.getString(3),
                                    rs3.getString(4));
                        }

                    }
//                    {
//                        System.out.println("\nNot certified students:");
//                        ResultSet rs = showNonCertifiedStudents(conn).executeQuery();
//                        while (rs.next()) {
//                            System.out.printf("%s: \"%s\"\n", rs.getString(1), rs.getString(2));
//                        }
//                    }
//                    {
//                        ResultSet rs = new Student(
//                                "Александр",
//                                "Ефимов",
//                                761805,
//                                "Пушкин, конечно, герой, но зачем стулья ломать?",
//                                "",
//                                "",
//                                ""
//
//                        ).insert(conn).executeQuery();
//                        while (rs.next()) {
//                            System.out.printf("Student added: %s %s\n", rs.getString(1),rs.getString(2));
//                        }
//                    }
//                    updateEfimov(conn).executeQuery();
//                    {
//                        System.out.println("\nPassed with new rules:");
//                        ResultSet rs = newCertificate(conn).executeQuery();
//                        while (rs.next()) {
//                            System.out.printf("Student :%s %s\n", rs.getString(1), rs.getString(2));
//                        }
//                    }
//                    updateNewCertificate(conn).executeQuery();
//                    {
//                        System.out.println("\nTable:");
//                        ResultSet rs = showTable(conn).executeQuery();
//                        while (rs.next()) {
//                            System.out.printf("Student :%s %s %s\n", rs.getString(1), rs.getString(2),rs.getString(7));
//                        }
//                    }
//                    deleteFailed(conn).executeQuery();
//                    {
//                        System.out.println("\nFinal table:");
//                        ResultSet rs = showTable(conn).executeQuery();
//                        while (rs.next()) {
//                            System.out.printf("Student :%s %s %s\n", rs.getString(1), rs.getString(2),rs.getString(7));
//                        }
//                    }
//

                }
                catch (SQLException ex) {
                    System.out.printf("Statement execution error: %s\n", ex);
                }
            }
            catch (SQLException ex) {
                System.out.printf("Can't create statement: %s\n", ex);
            }
        }
        catch (SQLException ex) {
            System.out.printf("Can't create connection: %s\n", ex);
        }
    }

    private static PreparedStatement createTable(Connection conn) throws SQLException {
        String state = "CREATE TABLE IF NOT EXISTS solutions2\n" +
                "(\n" +
                "studentID INT,\n" +
                "studentName CHAR(127) NOT NULL,\n" +
                "studentSurname CHAR(127) NOT NULL,\n" +
                "solutionID INT PRIMARY KEY,\n" +
                "reviewerID INT,\n" +
                "reviewerSurname CHAR(127),\n" +
                "reviewerDepartment CHAR(127),\n" +
                "score DOUBLE,\n" +
                "has_pass BOOLEAN\n" +
                ")\n" +
                "CHARACTER SET utf8\n" +
                "COLLATE utf8_general_ci;";
        return conn.prepareStatement(state);
    }

    private static PreparedStatement dropTable(Connection conn) throws SQLException {
        return conn.prepareStatement("DROP TABLE IF EXISTS solutions2");
    }

    private static ArrayList<PreparedStatement> insertSolutions(Connection conn){
        ArrayList<PreparedStatement> queries = new ArrayList<>();
        try {
            CSVReader reader = new CSVReader(new FileReader("data.csv"));
            String[] line = reader.readNext();
            line = reader.readNext();
            while(line!=null){
                Solution solution = new Solution(
                        Integer.parseInt(line[0]),
                        line[1],
                        line[2],
                        Integer.parseInt(line[3]),
                        Integer.parseInt(line[4]),
                        line[5],
                        line[6],
                        line[7],
                        line[8]
                );
                queries.add(solution.insert(conn));
                line = reader.readNext();
            }
        }catch (IOException  e){
            System.err.println("File not found");
        }
        catch (CsvValidationException e){
            System.err.println("CSV file is not validating");
        }
        catch (Exception e){
            System.err.println(e.getStackTrace());
        }

        return queries;
    }

    private static PreparedStatement showTable(Connection conn) throws SQLException {
        return conn.prepareStatement("SELECT studentID, studentName,studentSurname,solutionID,reviewerID,reviewerSurname, reviewerDepartment, score,has_pass FROM solutions2");
    }
    private static PreparedStatement selectMaxScore(Connection conn) throws SQLException {
        return conn.prepareStatement("SELECT studentID, studentName, studentSurname, score FROM solutions2 \n" +
                "WHERE score = (SELECT MAX(score) FROM solutions2)");
    }

    private static PreparedStatement topTeachers(Connection conn) throws SQLException {
        return conn.prepareStatement("SELECT reviewerID, reviewerSurname, AVG(score) AS av FROM solutions2\n" +
                "GROUP BY reviewerID\n" +
                "ORDER BY av DESC LIMIT 1");
    }
    private static PreparedStatement dnoTeachers(Connection conn) throws SQLException {
        return conn.prepareStatement("SELECT reviewerID, reviewerSurname, AVG(score) AS av FROM solutions2\n" +
                "GROUP BY reviewerID\n" +
                "ORDER BY av ASC LIMIT 1");
    }

    private static PreparedStatement selectMinScore(Connection conn) throws SQLException {
        return conn.prepareStatement("SELECT studentID, studentName, studentSurname, score FROM solutions2 \n" +
                "WHERE score = (SELECT MIN(score) FROM solutions2)");
    }
    private static PreparedStatement selectAssigStudBuTeach(Connection conn) throws SQLException {
        return conn.prepareStatement("SELECT studentID, studentName, studentSurname, score, reviewerSurname  FROM solutions2\n" +
                "ORDER BY studentID, score  DESC  ");
    }

    private static PreparedStatement selectMany(Connection conn) throws SQLException{
        return conn.prepareStatement("WITH s as\n" +
                "        (SELECT studentID, studentName, studentSurname, COUNT(reviewerSurname) as cnt, reviewerSurname as sur   FROM solutions2\n" +
                "    GROUP BY studentID, reviewerSurname\n" +
                "        ORDER BY cnt DESC , studentName)\n" +
                "        SELECT studentID, studentSurname, MAX(cnt), sur FROM  s\n" +
                "        GROUP BY studentID ");
    }

    private static PreparedStatement showNonCertifiedStudents(Connection conn) throws SQLException {
        return conn.prepareStatement("SELECT surname, answer FROM solutions WHERE has_pass != true OR has_pass IS NULL ");
    }

    private static PreparedStatement updateEfimov(Connection conn) throws SQLException {
        return conn.prepareStatement("UPDATE solutions SET score = 4.1, review = 'Стулья ломать, действительно, незачем' WHERE card = 761805 ");
    }

    private static PreparedStatement newCertificate(Connection conn) throws SQLException {
        return conn.prepareStatement("SELECT surname, card FROM solutions WHERE (has_pass != true OR has_pass IS NULL) AND score >= 2");
    }

    private static PreparedStatement updateNewCertificate(Connection conn) throws SQLException {
        return conn.prepareStatement("UPDATE solutions SET has_pass = true WHERE (has_pass != true OR has_pass IS NULL) AND score >= 3");
    }

    private static PreparedStatement deleteFailed(Connection conn) throws SQLException {
        return conn.prepareStatement("DELETE FROM solutions WHERE has_pass != true");
    }


    private static Connection createConnection() throws SQLException {
        return DriverManager.getConnection(URL, user, password);
    }

}


//    SELECT studentName, studentSurname, COUNT(reviewerSurname) as cnt, reviewerSurname   FROM solutions2
//        GROUP BY studentID, reviewerSurname
//        ORDER BY cnt DESC , studentName

//    WITH s as
//        (SELECT studentID, studentName, studentSurname, COUNT(reviewerSurname) as cnt, reviewerSurname as sur   FROM solutions2
//    GROUP BY studentID, reviewerSurname
//        ORDER BY cnt DESC , studentName)
//        SELECT studentID, studentSurname, MAX(cnt), sur FROM  s
//        GROUP BY studentID
