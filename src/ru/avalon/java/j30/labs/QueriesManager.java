package ru.avalon.java.j30.labs;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;

public class QueriesManager {

    private static final String queriesPath = "resources/queries.sql";

    private QueriesManager() {
    }

    /**
     * Читает текст файла с запросами
     *
     * @param sqlQueriesPath
     * @return LinkedList<String> sqlLines
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static LinkedList<String> getSQLLines() throws FileNotFoundException, IOException {
        LinkedList<String> sqlLines = new LinkedList<>();

        try (InputStream in = ClassLoader.getSystemResourceAsStream(queriesPath);
                Reader reader = new InputStreamReader(in);
                BufferedReader br = new BufferedReader(reader)) {
            String s;
            while ((s = br.readLine()) != null) {
                sqlLines.add(s);
            }

            return sqlLines;
        }
    }

    /**
     * Разбирает строку с запросами в HashMap<Название запроса, Запрос>
     *
     * @param queries
     * @return
     */
    private static HashMap<String, String> parseQueries(LinkedList<String> queries) {
        HashMap<String, String> sql = new HashMap<>();
        boolean entryStarted = false;
        String key = null;
        StringBuilder value = new StringBuilder();

        for (String line : queries) {

            if (line.startsWith("-->")) {
                entryStarted = true;
                key = line.substring(3).trim();
            } else {
                if (entryStarted) {
                    value.append(line);
                }
                if (line.trim().endsWith(";")) {
                    entryStarted = false;
                    sql.put(key, value.toString());
                    value.delete(0, value.length());
                }

            }
        }
        return sql;
    }
    
    /**
     * Возвращает подготовленный запрос
     * @param queryName ключ в HashMap<Название запроса, Запрос>, по которому получаем текст SQL запроса
     * @return PreparedStatement
     * @throws IOException
     * @throws SQLException 
     */
    public static PreparedStatement getPreparedStatement(Connection connection, String queryName) throws IOException, SQLException {
        String query = parseQueries(getSQLLines()).get(queryName).replace(";", "");
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        return preparedStatement;
    }
}
