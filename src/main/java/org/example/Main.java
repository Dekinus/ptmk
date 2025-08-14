package org.example;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Scanner;


public class Main {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "postgres";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Укажите режим работы приложения (1-6)");
            return;
        }

        String mode = args[0];


        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            switch (mode) {
                case "1":
                    createTable(connection);
                    break;
                case "2":
                    createRecord(connection);
                    break;
                case "3":
                    displayRecords(connection);
                    break;
                case "4":
                    fillRecords(connection);
                    break;
                case "5":
                    selectMaleF(connection);
                    break;
                case "6":
                    optimizeDB(connection);
                    break;
                default:
                    System.out.println("Неверный режим.");
            }
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS employees (" +
                "id SERIAL PRIMARY KEY," +
                "full_name VARCHAR(255)," +
                "birth_date DATE," +
                "gender VARCHAR(10)" +
                ")";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
            System.out.println("Таблица 'employees' создана.");
        }
    }

    private static void createRecord(Connection connection) throws SQLException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Введите ФИО: ");
        String fullName = scanner.nextLine();
        System.out.print("Введите дату рождения (YYYY-MM-DD): ");
        String birthDateStr = scanner.nextLine();
        LocalDate birthDate = LocalDate.parse(birthDateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        System.out.print("Введите пол (M/F): ");
        String gender = scanner.nextLine();

        Employee employee = new Employee(fullName, Date.valueOf(birthDate), gender);

        int age = employee.calculateAge();
        System.out.println("Полных лет: " + age);
        employee.insertIntoDB(connection);
    }

    private static void displayRecords(Connection connection) throws SQLException {
        String selectSQL = "SELECT DISTINCT ON (full_name, birth_date) full_name, birth_date, gender FROM employees ORDER BY full_name";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(selectSQL)) {
            System.out.println("Сотрудники:");
            System.out.println("-------------------------------------------------------");
            System.out.printf("%-23s %-15s %-6s %-10s%n", "ФИО", "Дата рождения", "Пол", "Возраст");
            System.out.println("-------------------------------------------------------");
            int i = 0;
            while (resultSet.next()) {
                String fullName = resultSet.getString("full_name");
                Date birthDate = resultSet.getDate("birth_date");
                String gender = resultSet.getString("gender");
                int age = Employee.calculateAge(birthDate);
                i++;
                System.out.printf("%-23s %-15s %-6s %-10d%n", fullName, birthDate, gender, age);
            }
            System.out.println(i);
        }
    }

    private static void fillRecords(Connection connection) throws SQLException {
        Random rand = new Random();
        List<Employee> employees = new ArrayList<>();
        int total = 1000000;
        int perGender = total / 2;
        int perLetter = perGender / 26;
        String[] genders = {"M", "F"};
        for (String gender : genders) {
            for (char c = 'A'; c <= 'Z'; c++) {
                for (int i = 0; i < perLetter; i++) {
                    String surname = c + generateRandomString(rand, 5);
                    String name = generateRandomString(rand, 4);
                    String patronymic = generateRandomString(rand, 6);
                    String fullName = surname + " " + name + " " + patronymic;
                    LocalDate birthDate = generateRandomDate(rand);
                    employees.add(new Employee(fullName, Date.valueOf(birthDate), gender));

                }
            }
        }

        for (int i = 0; i < 100; i++) {
            String surname = "F" + generateRandomString(rand, 5);
            String name = generateRandomString(rand, 4);
            String patronymic = generateRandomString(rand, 6);
            String fullName = surname + " " + name + " " + patronymic;
            LocalDate birthDate = generateRandomDate(rand);
            employees.add(new Employee(fullName, Date.valueOf(birthDate), "M"));
        }
        Collections.shuffle(employees);

        Employee.batchInsert(connection, employees);
        System.out.println("Добавлено " + employees.size() + " записей.");
    }

    private static String generateRandomString(Random rand, int length) {
        String chars = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(rand.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private static LocalDate generateRandomDate(Random rand) {
        int year = 1950 + rand.nextInt(50);
        int month = 1 + rand.nextInt(12);
        int day = 1 + rand.nextInt(28);
        return LocalDate.of(year, month, day);
    }

    private static void selectMaleF(Connection connection) throws SQLException {

        String query = "SELECT full_name, birth_date, gender FROM employees WHERE gender = 'M' AND full_name LIKE 'F%'";
        int count = 0;
        long startTime = System.currentTimeMillis();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
            while (rs.next()) {
                count++;
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Найдено " + count + " записей.");
        System.out.println("Время выполнения: " + (endTime - startTime) + " мс");
    }

    private static void optimizeDB(Connection connection) throws SQLException {
        String createIndexSQL = "CREATE INDEX IF NOT EXISTS idx_gender_fullname " +
                "ON employees (gender, full_name)";

        try (Statement statement = connection.createStatement()) {
            statement.execute(createIndexSQL);
            System.out.println("Создан индекс для таблицы employees");
            System.out.println("Выполнение задания 5 с индексом");
        }
        selectMaleF(connection);
    }
}
