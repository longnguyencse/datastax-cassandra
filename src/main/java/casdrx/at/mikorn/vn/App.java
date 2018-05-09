package casdrx.at.mikorn.vn;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Author Mikorn vietnam
 * Created on 27-Apr-18.
 */
public class App {
//    private static String CASSANDRA_CONTACT_POINTS = "192.168.1.146";
    private static String CASSANDRA_CONTACT_POINTS = "192.168.1.158";
    private static String KEY_SPACE = "ifp_version";
    private static String USER_NAME = "ifp";
    private static String PASSWORD = "test";


    private static enum OrgState {
        READY ("Ready"),
        ARCHIVED ("Archived"),
        UPLOADING ("Uploading"),
        DOWNLOADING ("Downloading");

        String state;
        OrgState(String state) {
            this.state = state;
        }

        public String getState() {
            return state;
        }
    }

    private static enum AnnotateStatus {
        NONE ("None"),
        NEW ("New"),
        ANNOTATING ("Annotating"),
        ANNOTATED ("Annotated"),
        CHECKING ("Checking"),
        FAILED ("Failed"),
        PASSED ("Passed");

        String state;
        AnnotateStatus(String state) {
            this.state = state;
        }
    }

    private static enum VersionState {
        READY ("Ready"),
        ARCHIVED ("Archived"),
        UPLOADING ("Uploading"),
        DOWNLOADING ("Downloading");

        String state;

        VersionState(String state) {
            this.state = state;
        }

        public String getState() {
            return state;
        }
    }

    public static void main(String[] args) {        // connect cassandra by cassandra drx
        Cluster cluster = Cluster.builder()
                .addContactPoint(CASSANDRA_CONTACT_POINTS)
                .withCredentials(USER_NAME, PASSWORD)
                .build();

        //session
        final Session session = cluster.connect(KEY_SPACE);
//        File file = new File("D:\\learn\\data\\example\\day");
//        final File file = new File("D:\\learn\\data\\example\\ngay_1");
        final File file = new File("D:\\learn\\data\\example\\ex2");
//        File file = new File("D:\\learn\\data\\example\\ex3_1");
//        File file = new File("D:\\learn\\data\\example\\ex400");
//        File file = new File("D:\\learn\\data\\example\\ex500");
//        final File file = new File("D:\\learn\\data\\example\\zz");
//        final File file = new File("\\\\192.168.1.201\\Public\\Picture\\Mui Ne - 06_02_2012\\K.Anh's Camera");
//        final File file = new File("\\\\192.168.1.201\\Public\\Picture\\Mui Ne - 06_02_2012\\Michael's Camera");
//        final File file = new File("\\\\192.168.1.201\\Public\\Picture\\Mui Ne - 06_02_2012\\Beauty Collection (Minh's cam)");
//        final File file = new File("\\\\192.168.1.201\\Public\\Picture\\Mui Ne - 06_02_2012\\Anh's Camera");

        final App app = new App();

        Runnable task = ()-> {
            app.dummyData4AnnotatedBin(session, file);
        };

        Runnable task1 = () -> {
            app.dummyData4OriginalBinData(session, file);
        };

        task.run();
        task1.run();

        Thread thread = new Thread(task);
        Thread thread1 = new Thread(task1);
        thread.start();
        thread1.start();

//        Thread thread = new Thread(new Runnable() {
//            public void run() {
//                app.dummyData4AnnotatedBin(session, file);
//            }
//        });
//        Thread thread1 = new Thread(new Runnable() {
//            public void run() {
//                app.dummyData4OriginalBinData(session, file);
//            }
//        });
//        thread.start();
//        thread1.start();
//        app.dummyData4OriginalBinData(session, file);
//        app.dummyData4AnnotatedBin(session, file);
       // File
//        File file = new File("src/main/resources/00.jpg");
//
//        try {
//            InputStream inputStream = new FileInputStream(file);
//            final FileChannel fc = new FileInputStream(inputStream).getChannel();
//            byte[] bytes =  IOUtils.toByteArray(inputStream);;
//
//            Insert insert = QueryBuilder.insertInto(KEY_SPACE, "annotated_bin_data_by_file_name_and_version")
//                    .value("file_name","longdarx")
//                    .value("frame_index", 0)
//                    .value("cur_version", 0)
//                    //.value("created_time", "")
//                    .value("prev_version", 0)
//                    .value("annotated_frame_unit_extension", "")
//                    .value("version_buffer_flag", false)
//                    .value("version_last_accessed_time", System.currentTimeMillis())
//                    .value("annotated_frame_raw_data", ByteBuffer.wrap(bytes));
//            System.out.println(insert.toString());
//
//            ResultSet result = session.execute(insert.toString());
//            System.out.println(result.wasApplied());
//            System.out.println(result);
//
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        System.exit(0);
    }

    private void close() {
        // session close

        // cluster close

    }

    public void dummyData4AnnotatedBin(Session session, final File folder) {
        for (final File fileEntry: folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                dummyData4AnnotatedBin(session, fileEntry);
            } else {
                // todo my suft
                System.out.println(fileEntry.getName());
                try {
                    InputStream inputStream = new FileInputStream(fileEntry);
                    byte[] bytes =  IOUtils.toByteArray(inputStream);;

                    Insert insert = QueryBuilder.insertInto(KEY_SPACE, "annotated_bin_data_by_file_name_and_version")
                            .value("file_name",fileEntry.getName())
                            .value("frame_index", 0)
                            .value("cur_version", 0)
                            .value("prev_version", 0)
                            .value("annotated_frame_unit_extension",
                                    FilenameUtils.getExtension(fileEntry.getAbsolutePath()))
                            .value("version_buffer_flag", false)
                            .value("version_ready_state", VersionState.UPLOADING.getState())
                            .value("version_last_accessed_time", System.currentTimeMillis())
                            .value("annotated_frame_raw_data", ByteBuffer.wrap(bytes));

                    ResultSet result = session.execute(insert.toString());
                    System.out.println(String.format("Time: %s, is Exhausted: ", System.currentTimeMillis(), result.isExhausted()));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void dummyData4OriginalBinData(Session session, File fileinfo) {
        for (final File fileEntry: fileinfo.listFiles()) {
            if (fileEntry.isDirectory()) {
                dummyData4AnnotatedBin(session, fileEntry);
            } else {
                // todo my suft
                try {
                    InputStream inputStream = new FileInputStream(fileEntry);
                    byte[] bytes =  IOUtils.toByteArray(inputStream);;

                    Insert insert = QueryBuilder.insertInto(KEY_SPACE, "orig_bin_data_by_file_name")
                            .value("file_name",fileEntry.getName())
                            .value("frame_index", 0)
                            .value("org_file_ready_state", VersionState.UPLOADING.getState())
                            .value("orig_frame_unit_extension",
                                    FilenameUtils.getExtension(fileEntry.getAbsolutePath()))
                            .value("org_file_buffer_flag", false)
                            .value("org_file_last_accessed_time", System.currentTimeMillis())
                            .value("orig_frame_raw_data", ByteBuffer.wrap(bytes));

                    ResultSet result = session.execute(insert.toString());
                    System.out.println(String.format("Time: %s, is Exhausted: ", System.currentTimeMillis(), result.isExhausted()));

                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
