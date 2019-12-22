package ru.avalon.java.j30.labs;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;

/**
 * Класс описывает представление о коде товара и отражает соответствующую 
 * таблицу базы данных Sample (таблица PRODUCT_CODE).
 * 
 * @author Daniel Alpatov <danial.alpatov@gmail.com>
 */
public class ProductCode {
    /**
     * Код товара
     */
    private String code;
    /**
     * Кода скидки
     */
    private char discountCode;
    /**
     * Описание
     */
    private String description;
    /**
     * Основной конструктор типа {@link ProductCode}
     * 
     * @param code код товара
     * @param discountCode код скидки
     * @param description описание 
     */
    public ProductCode(String code, char discountCode, String description) {
        this.code = code;
        this.discountCode = discountCode;
        this.description = description;
    }
    /**
     * Инициализирует объект значениями из переданного {@link ResultSet}
     * 
     * @param set {@link ResultSet}, полученный в результате запроса, 
     * содержащего все поля таблицы PRODUCT_CODE базы данных Sample.
     */
    private ProductCode(ResultSet set) throws SQLException {
        /*
         * TODO #05 реализуйте конструктор класса ProductCode
         */
    }
    /**
     * Возвращает код товара
     * 
     * @return Объект типа {@link String}
     */
    public String getCode() {
        return code;
    }
    /**
     * Устанавливает код товара
     * 
     * @param code код товара
     */
    public void setCode(String code) {
        this.code = code;
    }
    
    /**
     * Возвращает код скидки
     * 
     * @return Объект типа {@link String}
     */
    public char getDiscountCode() {
        return discountCode;
    }
    /**
     * Устанавливает код скидки
     * 
     * @param discountCode код скидки
     */
    public void setDiscountCode(char discountCode) {
        this.discountCode = discountCode;
    }
    /**
     * Возвращает описание
     * 
     * @return Объект типа {@link String}
     */
    public String getDescription() {
        return description;
    }
    /**
     * Устанавливает описание
     * 
     * @param description описание
     */
    public void setDescription(String description) {
        this.description = description;
    }
    /**
     * Хеш-функция типа {@link ProductCode}.
     * 
     * @return Значение хеш-кода объекта типа {@link ProductCode}
     */
    @Override
    public int hashCode() {
        /*
         * TODO #06 Реализуйте метод hashCode
         */
        return Objects.hash(code, discountCode, description);
    }
    /**
     * Сравнивает некоторый произвольный объект с текущим объектом типа 
     * {@link ProductCode}
     * 
     * @param obj Объект, скоторым сравнивается текущий объект.
     * @return true, если объект obj тождественен текущему объекту. В обратном 
     * случае - false.
     */
    @Override
    public boolean equals(Object obj) {
        /*
         * TODO #07 Реализуйте метод equals
         */
        if (this == obj) return true;
        if (obj instanceof ProductCode) {
            ProductCode other = (ProductCode) obj;
            return (this.code.equals(other.code)) 
                    && (this.discountCode == other.discountCode) 
                    && (this.description.equals(other.description));
        } return false;
    }
    /**
     * Возвращает строковое представление кода товара.
     * 
     * @return Объект типа {@link String}
     */
    @Override
    public String toString() {
        /*
         * TODO #08 Реализуйте метод toString
         */
        return "Code: " + code + " | Discount code: " + discountCode + " | Description: " + description;
    }
    /**
     * Возвращает запрос на выбор всех записей из таблицы PRODUCT_CODE 
     * базы данных Sample
     * 
     * @param connection действительное соединение с базой данных
     * @return Запрос в виде объекта класса {@link PreparedStatement}
     */
    public static PreparedStatement getSelectQuery(Connection connection) throws SQLException {
        /*
         * TODO #09 Реализуйте метод getSelectQuery
         */
        String query = "SELECT * FROM PRODUCT_CODE";
        PreparedStatement preparedStatement = connection.prepareStatement(query); 
        return preparedStatement;
    }
    /**
     * Возвращает запрос на добавление записи в таблицу PRODUCT_CODE 
     * базы данных Sample
     * 
     * @param connection действительное соединение с базой данных
     * @return Запрос в виде объекта класса {@link PreparedStatement}
     */
    public static PreparedStatement getInsertQuery(Connection connection) throws SQLException {
        /*
         * TODO #10 Реализуйте метод getInsertQuery
         */
        String query = "INSERT INTO PRODUCT_CODE (PROD_CODE, DISCOUNT_CODE, DESCRIPTION)"
                     + "VALUES (?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        return preparedStatement;
    }
    /**
     * Возвращает запрос на обновление значений записи в таблице PRODUCT_CODE 
     * базы данных Sample
     * 
     * @param connection действительное соединение с базой данных
     * @return Запрос в виде объекта класса {@link PreparedStatement}
     */
    public static PreparedStatement getUpdateQuery(Connection connection) throws SQLException {
        /*
         * TODO #11 Реализуйте метод getUpdateQuery
         */
        String query = "UPDATE PRODUCT_CODE SET PROD_CODE = ?"
                                             + "DISCOUNT_CODE = ?, "
                                             + "DESCRIPTION = ? "
                                             + "WHERE PROD_CODE = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        return preparedStatement;
        
    }
    /**
     * Преобразует {@link ResultSet} в коллекцию объектов типа {@link ProductCode}
     * 
     * @param set {@link ResultSet}, полученный в результате запроса, содержащего 
     * все поля таблицы PRODUCT_CODE базы данных Sample
     * @return Коллекция объектов типа {@link ProductCode}
     * @throws SQLException 
     */
    public static Collection<ProductCode> convert(ResultSet set) throws SQLException {
        /*
         * TODO #12 Реализуйте метод convert
         */
        Collection<ProductCode> collection = new LinkedList<>();
        while (set.next()) {
            String code = set.getString("PROD_CODE");
            String discountCode = set.getString("DISCOUNT_CODE");
            String description = set.getString("DESCRIPTION");
            collection.add(new ProductCode(code, discountCode.charAt(0), description));
        }
        return new ArrayList<ProductCode>(collection);
    }
    /**
     * Сохраняет текущий объект в базе данных. 
     * <p>
     * Если запись ещё не существует, то выполняется запрос типа INSERT.
     * <p>
     * Если запись уже существует в базе данных, то выполняется запрос типа UPDATE.
     * 
     * @param connection действительное соединение с базой данных
     */
    public void save(Connection connection) throws SQLException {
        /*
         * TODO #13 Реализуйте метод convert
         */
        String selectQuery = "SELECT * FROM PRODUCT_CODE WHERE PROD_CODE = '" + this.code + "'";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(selectQuery)) {
                while (resultSet.next()) {
                    System.out.println(resultSet.getString("PROD_CODE"));
                }
                if (!resultSet.next()) {
                    System.out.println("ras");
                    PreparedStatement ps = getInsertQuery(connection);
                    ps.setString(1, code);
                    ps.setString(2, String.valueOf(discountCode));
                    ps.setString(3, description);
                    ps.execute();
                } else {
                    System.out.println("dvas");
                    PreparedStatement ps = getUpdateQuery(connection);
                    ps.setString(1, String.valueOf(code));
                    ps.setString(2, String.valueOf(discountCode));
                    ps.setString(3, description);
                    ps.setString(4, code);
                    ps.executeUpdate();
                }
            }
        }
    }
    /**
     * Возвращает все записи таблицы PRODUCT_CODE в виде коллекции объектов
     * типа {@link ProductCode}
     * 
     * @param connection действительное соединение с базой данных
     * @return коллекция объектов типа {@link ProductCode}
     * @throws SQLException 
     */
    public static Collection<ProductCode> all(Connection connection) throws SQLException {
        try (PreparedStatement statement = getSelectQuery(connection)) {
            try (ResultSet result = statement.executeQuery()) {
                return convert(result);
            }
        }
    }
}
