import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;


import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.sqlite.JDBC.createConnection;


public class Main {
     public static void main(String[] args) throws Exception {
         String DEFAULT_DRIVER = "org.sqlite.JDBC";
         String DEFAULT_URL = "jdbc:sqlite:ms3db.db";

         Connection conn = createConnection(DEFAULT_DRIVER, DEFAULT_URL);

        String file = "ms3Interview.csv";
        List<String[]> data = null;

        createTable(conn);

          try{
              CSVReader reader =  new CSVReaderBuilder(new FileReader(file)).withSkipLines(1).build();
              data = reader.readAll();
              reader.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }

          //create new arraylist for bad data
            List<String[]> bad = new ArrayList<String[]>();
            for(String[] row:new ArrayList<String[]>(data)) {
                for (String s :row)
                    if(s.length() == 0){
                        bad.add(row);
                        data.remove(row);
                        break;
                    }
            }
            //write bad data to csv file
            writeBadData(bad);
            printStats(data.size(), bad.size());
            //upload data to sqlite
            uploadToSQLite(conn, data);
            //List<String[]> records = getAllRecords(conn);   //fetches all the records from the database


    }


    static Connection createConnection(String driver, String url)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }


    public static void printStats(int dataCount, int badDataCount) throws Exception{
        PrintWriter pw = new PrintWriter("stats.log");
        try {
            String totalStat = "# of records received: " +(dataCount+badDataCount);
            String totalSuccessful = "# of records successful: " + dataCount;
            String totalFailed = "# of records failed: " + badDataCount;
            System.out.println(totalStat+"\n"+totalSuccessful+"\n"+totalFailed);

            pw.println(totalStat);
            pw.println(totalSuccessful);
            pw.println(totalFailed);
            pw.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void writeBadData(List<String[]> badData) throws Exception {
        //create simple date format and timestamp
        SimpleDateFormat sdf = new SimpleDateFormat("yyy.MM.dd-HH.mm.ss");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String filename = "bad-data-" + sdf.format(timestamp) + ".csv";

        CSVWriter writer = new CSVWriter(new FileWriter(filename));
        String[] header = {"A","B","C","D","E","F","G","H","I","J"};
        writer.writeNext(header);

        for(String[] s:badData) {
            writer.writeNext(s);
        }

        writer.close();

    }


    public static void createTable(Connection conn) throws SQLException {
        final String sql = "CREATE TABLE  Data (A VARCHAR(100) ,"
                + "B VARCHAR(100),C VARCHAR(100),D VARCHAR(100),E VARCHAR(1000),"
                + "F VARCHAR(100),G DECIMAL ,H VARCHAR(5) ,I VARCHAR(5),J VARCHAR(100))";
        Statement statement = null;
        try {

            statement = conn.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS Data;");
            statement.execute(sql);
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static List<String[]> getAllRecords(Connection conn) throws SQLException{
        //query sqlite with the following command and save resulsts in  resultset
        final String sql = "SELECT * FROM Data";
        ResultSet rs = null;
        List<String[]> result = new ArrayList<>();
        try {

            rs = conn.createStatement().executeQuery("SELECT * FROM Data");
            while (rs.next()) {
                //append all records into a string array
                String[] row = {rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10)};
                //print the records
                result.add(row);
                //System.out.println(Arrays.toString(row));
            }

        }catch (SQLException e){
            e.printStackTrace();
        }
        finally {
            conn.close();
        }

        return result;
    }

    public static void uploadToSQLite(Connection conn,List<String[]> data) throws Exception {

        //createTable(conn);
        final String sql = "INSERT INTO Data (A,B,C,D,E,F,G,H,I,J) VALUES (?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = null;
        try{
            ps = conn.prepareStatement(sql);
            for(String[] d :data) {
                ps.setString(1, d[0]);
                ps.setString(2, d[1]);
                ps.setString(3, d[2]);
                ps.setString(4, d[3]);
                ps.setString(5, d[4]);
                ps.setString(6, d[5]);
                ps.setString(7, d[6].substring(1)); //no need to store $ sign in the db
                ps.setString(8, d[7]);
                ps.setString(9, d[8]);
                ps.setString(10, d[9]);
                ps.addBatch();
            }
            conn.setAutoCommit(false);
            ps.executeBatch();
            //conn.setAutoCommit(true);
            conn.commit();
            ps.close();
            conn.close();

        }
        catch (SQLException ex){
            ex.printStackTrace();
        }

        }

}
