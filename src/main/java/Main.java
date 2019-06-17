import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static String JDBC= "jdbc:sqlite:ms3,db";
    public static void main(String[] args) throws Exception {
        String file = "ms3Interview.csv";
        List<String[]> data = null;

          try{
              CSVReader reader =  new CSVReaderBuilder(new FileReader(file)).withSkipLines(1).build();
              data = reader.readAll();
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
            //upload data to sqlite
            uploadToSQLite(data);
            //write bad data to csv file
            writeBadData(bad);
            printStats(data.size(), bad.size());
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

    public static void uploadToSQLite(List<String[]> data) throws SQLException {
        Connection conn = null;
        try{
            conn = DriverManager.getConnection(JDBC);
            conn.setAutoCommit(false);
            //create table if not exists
            conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS Data(A VARCHAR ,B VARCHAR,C VARCHAR,D VARCHAR,E VARCHAR,F VARCHAR,G DECIMAL ,H VARCHAR ,I VARCHAR,J VARCHAR)");
            //prepared statements for inserting into sqlite.
            PreparedStatement ps = conn.prepareStatement("INSERT INTO Data (A,B,C,D,E,F,G,H,I,J) VALUES (?,?,?,?,?,?,?,?,?,?)");

            for(String[] d :data){
                ps.setString(1,d[0]);
                ps.setString(2,d[1]);
                ps.setString(3,d[2]);
                ps.setString(4,d[3]);
                ps.setString(5,d[4]);
                ps.setString(6,d[5]);
                ps.setString(7, d[6].substring(1)); //no need to store $ sign in the db
                ps.setString(8,d[7]);
                ps.setString(9,d[8]);
                ps.setString(10,d[9]);
                ps.executeUpdate();
            }

            //query sqlite with the following command and save resulsts in  resultset
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM Data");
            while(rs.next()){
                //append all records into a string array
                String[] row = {rs.getString(1),rs.getString(2), rs.getString(3),rs.getString(4),rs.getString(6),rs.getString(7),rs.getString(8),rs.getString(9),rs.getString(10)};
                //print the records
                //System.out.println(Arrays.toString(row));
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        conn.close();
    }
}
