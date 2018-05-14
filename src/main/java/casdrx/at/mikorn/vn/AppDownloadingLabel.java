package casdrx.at.mikorn.vn;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Author Mikorn vietnam
 * Created on 09-May-18.
 */

public class AppDownloadingLabel implements ContantNameTable {
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
//        final File file = new File("D:\\learn\\data\\example\\ex2");
//        final File file = new File("src/main/resources/00.jpg");
//        final File file = new File("D:\\learn\\data\\example\\ex2");
        File file = new File("D:\\learn\\data\\example\\ex3_1");
        final AppDownloadingLabel appInnovation = new AppDownloadingLabel();

        try {
            appInnovation.getFileInDirectory(file, session);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
        System.exit(0);
    }

    private void getFileInDirectory(File folder, Session session) throws IOException {
        if (null == session) {
            return;
        }
        if (folder.isFile()) {
            InputStream inputStream = new FileInputStream(folder);
            byte[] bytes = IOUtils.toByteArray(inputStream);
            save2Db(session, bytes, folder);
            return;
        }
        for (File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                getFileInDirectory(fileEntry, session);
            } else {
                // file : fileEntry
                try {
                    InputStream inputStream = new FileInputStream(fileEntry);
                    byte[] bytes = IOUtils.toByteArray(inputStream);
                    // save data
                    save2Db(session, bytes, fileEntry);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private void save2Db(Session session, byte[] bytes, File fileEntry) {
        dumyData4MasterFile(session, bytes, fileEntry);
        // 2
        dumyData4OrigFile(session, bytes, fileEntry);
        // 3
        dumyData4OAnnotatedFile(session, bytes, fileEntry);
        // 4
        dumyData4GTdataFile(session, bytes, fileEntry);
        // 5
        dumyData4AnnotatedUpDownFile(session, bytes, fileEntry);
        // 6
        dumyData4OrigUpDownFile(session, bytes, fileEntry);
    }
    // insert TB_MASTER_FILE
    private void dumyData4MasterFile(Session session, byte[] bytes, File fileEntry)
            throws InvalidQueryException {
        if (null == session) {
            return;
        }
        Set<String> set = new HashSet<>(Arrays.asList(
                FilenameUtils.getBaseName(fileEntry.getAbsolutePath())
                        + "_0001." + FilenameUtils.getExtension(fileEntry.getAbsolutePath())
        ));
        Runnable runnable = () -> {
            Insert insert = QueryBuilder.insertInto(KEY_SPACE, TB_MASTER_FILE)
                    .value("file_name",fileEntry.getName())
                    .value("cur_version", 1)
                    .value("annotated_status", VersionState.ARCHIVED.getState())
                    .value("list_frame_names", set)
                    //annotated_status
                    .value("file_extension",
                            FilenameUtils.getExtension(fileEntry.getAbsolutePath()))
                    .value("org_file_created_time", System.currentTimeMillis())
                    .value("version_created_time", System.currentTimeMillis())
                    .value("total_frames", 1);

            ResultSet result = session.execute(insert.toString());

            System.out.println(String.format("Time: %s, is Exhausted: %s",
                    System.currentTimeMillis(), result.isExhausted()));

        };

        runnable.run();
        Thread thread = new Thread(runnable);
        thread.start();
    }

    // Insert TB_ANNOTATED_FILE
    private void dumyData4OAnnotatedFile(Session session, byte[] bytes, File fileEntry)
            throws InvalidQueryException{
        if (null == session) {
            return;
        }

        Runnable runnable = () -> {
            Insert insert = QueryBuilder.insertInto(KEY_SPACE, TB_ANNOTATED_FILE)
                    .value("file_name",fileEntry.getName())
                    .value("frame_name", fileEntry.getName() + FilenameUtils.getBaseName(fileEntry.getAbsolutePath())
                            + "_0001." + FilenameUtils.getExtension(fileEntry.getAbsolutePath()))
                    .value("cur_version", 1)
                    .value("annotated_frame_unit_extension",
                            FilenameUtils.getExtension(fileEntry.getAbsolutePath()))
                    .value("version_ready_state", VersionState.UPLOADING.getState())
//                    .value("version_last_accessed_time", System.currentTimeMillis())
                    .value("annotated_frame_raw_data", ByteBuffer.wrap(bytes));

            ResultSet result = session.execute(insert.toString());

            System.out.println(String.format("Time: %s, is Exhausted: %s",
                    System.currentTimeMillis(), result.isExhausted()));

        };

        runnable.run();
        Thread thread = new Thread(runnable);
        thread.start();
    }

    // Insert data TB_ORIG_FILE

    private void dumyData4OrigFile(Session session, byte[] bytes, File fileEntry)
            throws InvalidQueryException{
        if (null == session) {
            return;
        }
        Runnable runnable = () -> {
            Insert insert = QueryBuilder.insertInto(KEY_SPACE, TB_ORIG_FILE)
                    .value("file_name",fileEntry.getName())
                    .value("frame_name", FilenameUtils.getBaseName(fileEntry.getAbsolutePath())
                            + "_0001." + FilenameUtils.getExtension(fileEntry.getAbsolutePath()))
//                    .value("cur_version", 1)
//                    .value("prev_version", 0)
                    .value("orig_frame_unit_extension",
                            FilenameUtils.getExtension(fileEntry.getAbsolutePath()))
//                    .value("version_buffer_flag", false)
                    .value("org_file_ready_state", VersionState.UPLOADING.getState())
//                    .value("version_last_accessed_time", System.currentTimeMillis())
                    .value("orig_frame_raw_data", ByteBuffer.wrap(bytes));

            ResultSet result = session.execute(insert.toString());

            System.out.println(String.format("Time: %s, is Exhausted: %s",
                    System.currentTimeMillis(), result.isExhausted()));

        };

        runnable.run();
        Thread thread = new Thread(runnable);
        thread.start();
    }

    // Insert data TB_GT_DATA
    private void dumyData4GTdataFile(Session session, byte[] bytes, File fileEntry)
            throws InvalidQueryException{
        if (null == session) {
            return;
        }
        Runnable runnable = () -> {
            Insert insert = QueryBuilder.insertInto(KEY_SPACE, TB_GT_DATA)
                    .value("file_name",fileEntry.getName())
                    .value("gt_timestamp", System.currentTimeMillis())
//                    .value("frame_index", 0)
                    .value("number_car", 3579)
                    .value("number_pedestrian", 0)
//                    .value("file_extension",
//                            FilenameUtils.getExtension(fileEntry.getAbsolutePath()))
                    .value("weather", "rain");

            ResultSet result = session.execute(insert.toString());

            System.out.println(String.format("Time: %s, is Exhausted: %s",
                    System.currentTimeMillis(), result.isExhausted()));

        };

        runnable.run();
        Thread thread = new Thread(runnable);
        thread.start();
    }


    // Insert data 4 TB_LIST_ORIG_UP_DOWN
    private void dumyData4OrigUpDownFile(Session session, byte[] bytes, File fileEntry)
            throws InvalidQueryException  {
        if (null == session) {
            return;
        }
        Set<String> set = new HashSet<>(Arrays.asList(
                FilenameUtils.getBaseName(fileEntry.getAbsolutePath())
                        + "_0001." + FilenameUtils.getExtension(fileEntry.getAbsolutePath())));

        Runnable runnable = () -> {
            Update.Where update = QueryBuilder.update(KEY_SPACE, TB_LIST_ORIG_UP_DOWN)
//                    .with(QueryBuilder.incr("total_finished_process_frames",0))
                    .with(QueryBuilder.set("list_remaining_frame_names",set))
                    .where(QueryBuilder.eq("org_file_ready_state", VersionState.DOWNLOADING.getState()))
                    .and(QueryBuilder.eq("org_file_buffer_flag", false))
                    .and(QueryBuilder.eq("total_frames", 1))
                    .and(QueryBuilder.eq("file_name",fileEntry.getName()));


            System.out.println(update.toString());
            ResultSet result = session.execute(update.toString());

            System.out.println(String.format("Time: %s, is Exhausted: %s",
                    System.currentTimeMillis(), result.isExhausted()));
        };

        runnable.run();
        Thread thread = new Thread(runnable);
        thread.start();
    }

    // Insert data 4 TB_LIST_ANNOTATED_UP_DOWN
    private void dumyData4AnnotatedUpDownFile(Session session, byte[] bytes, File fileEntry)
            throws InvalidQueryException{
        if (null == session) {
            return;
        }
        Set<String> set = new HashSet<>(Arrays.asList(
                FilenameUtils.getBaseName(fileEntry.getAbsolutePath())
                        + "_0001." + FilenameUtils.getExtension(fileEntry.getAbsolutePath())));

        Runnable runnable = () -> {
            Update.Where update = QueryBuilder.update(KEY_SPACE, TB_LIST_ANNOTATED_UP_DOWN)
//                    .with(QueryBuilder.incr("total_finished_process_frames",0))
                    .with(QueryBuilder.set("list_remaining_frame_names", set))

                    .where(QueryBuilder.eq("version_ready_state", VersionState.DOWNLOADING.getState()))
                    .and(QueryBuilder.eq("version_buffer_flag", false))
                    .and(QueryBuilder.eq("cur_version", 1))
                    .and(QueryBuilder.eq("total_frames", 1))
                    .and(QueryBuilder.eq("file_name",fileEntry.getName()));


            System.out.println(update.toString());
            ResultSet result = session.execute(update.toString());

            System.out.println(String.format("Time: %s, is Exhausted: %s",
                    System.currentTimeMillis(), result.isExhausted()));

        };

        runnable.run();
        Thread thread = new Thread(runnable);
        thread.start();
    }
}

