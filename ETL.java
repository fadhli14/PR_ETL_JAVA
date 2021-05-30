
import java.sql.*;
import java.io.*;

public class ETL {

    public static void  main (String[] args) {
        // username dan password untuk koneksi ke databse sql
        String userName = "postgresql";
        String password = "digitalskola";

        //database koneksi : koneksietl, maka dapat dituliskan setalah no. IP
        String  connectionString ="jdbc:postgresql://127.0.0.1:5432/connectionETL?user="+userName+"&password="+password;

        //koneksi ke database dengan menerapkan exception handling
        try {
            Connection conn = DriverManager.getConnection(connectionString);
            Statement stmnt = conn.createStatement();

            FileReader file = new FileReader(("D:\\data_pendidikan.csv"));
            BufferedReader reader = new BufferedReader(file);
            String lineText = null;

            reader.readLine();

            // untuk membaca file csv line byline dan memastikan tidak = null
            while ((lineText = reader.readLine()) != null) {
                //dibawah ini merupakan untuk input array data
                String[] data = lineText.split(",");
                int kode_provinsi = Integer.parseInt(data[0]);
                String nama_provinsi = data[1];
                String tingkat_pendidikan = data[2];
                String jenis_kelamin = data[3];
                int jumlah_individu = Integer.parseInt(data[4]);

                // String data untuk membaca data csv ke psql
                String Data = "INSERT INTO data_pendidikan VALUES ("+kode_provinsi+", '"+nama_provinsi+"', '"+tingkat_pendidikan+"', '"+jenis_kelamin+"', "+jumlah_individu+")";

                stmnt.executeUpdate(Data);

            }


            //jangan lupa untuk menutup proses query dan koneksi
            stmnt.close();
            conn.close();

        } catch (SQLException | IOException e) {
            e.printStackTrace();;
        }
    }

}
