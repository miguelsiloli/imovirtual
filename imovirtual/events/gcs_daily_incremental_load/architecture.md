# Mental Map: Scheduled Daily Load - Process Latest Parquet File

1.  **Trigger: Scheduled Time Reached**
    *   **What:** Daily schedule hits (e.g., configured time like 2 AM).
    *   **How:** Cloud Scheduler job.
    *   **Action:** Starts the Cloud Function/Cloud Run job.

2.  **Find the Latest File**
    *   **What:** Identify the single *most recent* Parquet file within the GCS directory (`gs://miguelsiloli-housing-data/staging/imovirtual/`).
    *   **How:**
        *   List all objects in the specified `GCS_SUBFOLDER_PATH`.
        *   Filter for files ending in `.parquet` (or your specific extension).
        *   **Sort** the list of Parquet files. The most reliable way is usually by **filename**, assuming they contain a sortable timestamp (e.g., `data_YYYYMMDDTHHMMSS.parquet`). Sorting by GCS object creation/update time can sometimes be less predictable.
        *   Select the **last file** from the sorted list.
    *   **Important:**
        *   A consistent, sortable filename convention is crucial for this to work reliably.
        *   Handle the case where *no* Parquet files are found (log and exit gracefully).

3.  **Process Target File**
    *   **What:** Read the *specific latest file* identified in Step 2 from GCS into memory (e.g., using Pandas).
    *   **How:** Use the GCS URI of the identified file.

4.  **Prep Data (If Needed)**
    *   **What:** Cleanse columns, fix data types, etc., for the data read in Step 3.
    *   **How:** Standard data manipulation.
    *   **Important:** Ensure `slug` and `ingestionDate` are ready for comparison.

5.  **Identify Keys in Target File**
    *   **What:** Get all unique `(slug, ingestionDate)` pairs from the data *just read* from the *latest* Parquet file.
    *   **How:** Extract these two columns, drop duplicates.

6.  **Check BigQuery for Existing Keys**
    *   **What:** Ask BigQuery: "Do any of the `(slug, ingestionDate)` pairs I found in step 5 *already exist* in the `staging` table?"
    *   **How:** Run a targeted SQL query against BigQuery using the keys from step 5 (e.g., `WHERE (slug, ingestionDate) IN UNNEST(...)`).
    *   **Important:** Get back a list of keys that *do* exist.

7.  **Filter Out Duplicates**
    *   **What:** Remove rows from the Parquet data (in memory) whose `(slug, ingestionDate)` pair was found in BigQuery (result of step 6).
    *   **How:** Filter the DataFrame/data structure.
    *   **Result:** Only rows that are truly *new* based on the composite key remain.

8.  **Append New Rows to BigQuery**
    *   **What:** Load the filtered (new-only) rows into the BigQuery `staging` table.
    *   **How:** Use the BigQuery client library's load function.
    *   **Important:** Use `WRITE_APPEND` and `CREATE_NEVER` as configured in your `.env`.

**Key Changes & Considerations for this Scheduled Flow:**

*   **Trigger:** Cloud Scheduler instead of Eventarc.
*   **File Discovery:** The function *must* actively list GCS objects and implement logic to find the "latest" file based on a clear convention (usually filename).
*   **Potential for Missed Data:** If multiple files arrive between scheduled runs, this approach will *only process the absolute latest one found during the run*, potentially skipping intermediate files. If processing *every* file is critical, a different approach (like event-driven or processing all *new* files since last run) would be needed.
*   **Idempotency:** The check in Step 6 ensures data isn't duplicated in BQ even if the same "latest" file is processed multiple times (e.g., due to a job failure and retry).
*   **Error Handling:** Need robust error handling, especially around listing files, sorting, and handling the "no files found" scenario.