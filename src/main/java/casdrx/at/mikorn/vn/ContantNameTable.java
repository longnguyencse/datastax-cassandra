package casdrx.at.mikorn.vn;

/**
 * Author Mikorn vietnam
 * Created on 09-May-18.
 */

public interface ContantNameTable {
    String TB_MASTER_FILE               = "file_and_version_info_by_file_name";
    String TB_ORIG_FILE                 = "orig_bin_data_by_file_name";
    String TB_ANNOTATED_FILE            = "annotated_bin_data_by_file_name_and_version";
    String TB_GT_DATA                   = "GT_data_by_file_name";

    String TB_LIST_ORIG_UP_DOWN         = "list_ready_upload_download_files";
    String TB_LIST_ANNOTATED_UP_DOWN    = "list_ready_upload_download_versions";

    String TB_MASTER_FILE_BY_ANNOTATED  = "file_and_version_info_by_annotated_status";
    String TB_ACCESSED_ACTIVITY         = "accessed_activity";


}
