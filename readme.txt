1. Create database "tiki_crawler"
2. Create table "tiki_product" - for valid records
3. Create table "tiki_error" - for error records (exception)
4. Create table "tiki_queue" - for importing product list & execute pending (checkpoint here)
5. Setup supervisord
6. Running main script
    - Deduplication
    - Progress bar
    - Batch summary
    - Total summary at the end
    - EST.Runtime ~ 1h
    - Total Processed: 200.000
        + Good Records (Including mising field ones) =  198.942
        + Exception (404 - Not found) = 1058
