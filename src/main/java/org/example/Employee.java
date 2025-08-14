package org.example;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Period;
import java.util.List;

class Employee {
    private String fullName;
    private Date birthDate;
    private String gender;

    public Employee(String fullName, Date birthDate, String gender) {
        this.fullName = fullName;
        this.birthDate = birthDate;
        this.gender = gender;
    }

    public void insertIntoDB(Connection connection) throws SQLException {
        String insertSQL = "INSERT INTO employees (full_name, birth_date, gender) VALUES (?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            preparedStatement.setString(1, fullName);
            preparedStatement.setDate(2, birthDate);
            preparedStatement.setString(3, gender);
            preparedStatement.executeUpdate();
            System.out.println("Record inserted successfully.");
        }
    }

    public int calculateAge() {
        LocalDate birthLocalDate = birthDate.toLocalDate();
        LocalDate currentDate = LocalDate.now();
        Period period = Period.between(birthLocalDate, currentDate);
        return period.getYears();
    }

    public static int calculateAge(Date birthDate) {
        LocalDate birthLocalDate = birthDate.toLocalDate();
        LocalDate currentDate = LocalDate.now();
        Period period = Period.between(birthLocalDate, currentDate);
        return period.getYears();
    }

    public static void batchInsert(Connection connection, List<Employee> employees) throws SQLException {
        String insertSQL = "INSERT INTO employees (full_name, birth_date, gender) VALUES (?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            for (Employee emp : employees) {
                preparedStatement.setString(1, emp.fullName);
                preparedStatement.setDate(2, emp.birthDate);
                preparedStatement.setString(3, emp.gender);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
    }
}