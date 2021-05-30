import java.sql.*;

class JavaDatabase {

    public static void main (String[] args) {
        String url = "jdbc:postgresql://127.0.0.1:5432/connectionETL"; // untuk connect pada network dan mengacu pada codingan Connection conn
        String user = "postgres";
        String password = "digitalskola";

        String createTableQuery ="CREATE TABLE ddk_tingkat_pendidikan" +
                "(kode_provinsi INTEGER not NULL, " +
                " nama_provinsi VARCHAR(255), "+
                " tingkat_pendidikan VARCHAR(255), " +
                " jenis_kelamin VARCHAR(255), " +
                " jumlah_individu INTEGER )";

        try {
            Connection conn = DriverManager.getConnection(url,user,password);
            Statement stmnt = conn.createStatement();
            stmnt.executeUpdate(createTableQuery);
            stmnt.close();
            conn.close();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}
