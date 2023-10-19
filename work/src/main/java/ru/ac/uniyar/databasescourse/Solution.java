package ru.ac.uniyar.databasescourse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Solution {
    public Integer studentID;
    public String name;
    public String surname;
    public Integer solutionID;
    public Integer reviewerID;
    public String  reviewerSurname;
    public String  reviewerDepartment;
    public Double score;
    public Boolean has_pass;

    public Solution(Integer studentID, String  name, String surname, Integer solutionID,
                   Integer reviewID, String reviewerSurname,
                   String reviewerDepartment, String score, String has_pass){
        this.name=name;
        this.studentID=studentID;
        this.surname=surname;
        this.solutionID=solutionID;
        this.reviewerID=reviewID;
        this.score=Double.parseDouble(score);
        this.reviewerSurname=reviewerSurname;
        this.reviewerDepartment=reviewerDepartment;
        if(has_pass.equals("No"))
            this.has_pass=Boolean.FALSE;
        else
            this.has_pass=Boolean.TRUE;
    }

    public PreparedStatement insert(Connection conn) throws SQLException {
        String state = "INSERT INTO solutions2(studentID, studentName,studentSurname,solutionID,reviewerID,reviewerSurname, reviewerDepartment, score,has_pass) VALUES (?,?,?,?,?,?,?,?,?)";
        PreparedStatement prep = conn.prepareStatement(state);
        prep.setInt(1,this.studentID);
        prep.setString(2,this.name);
        prep.setString(3,this.surname);
        prep.setInt(4,this.solutionID);
        prep.setInt(5,this.reviewerID);
        prep.setString(6,this.reviewerSurname);
        prep.setString(7,this.reviewerDepartment);
        prep.setDouble(8,this.score);
        prep.setBoolean(9,this.has_pass);
        return prep;
    }

}
