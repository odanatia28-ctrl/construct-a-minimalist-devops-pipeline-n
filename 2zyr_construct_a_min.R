# 2zyr_construct_a_min.R

# Load necessary libraries
library(RPostgres)
library(rjson)
library(httr)

# Define constants
DB_HOST <- "localhost"
DB_USER <- "devops"
DB_PASSWORD <- "devops"
DB_NAME <- "devops_db"

NOTIF_EMAIL <- "devops@example.com"
NOTIF_TOKEN <- "your-slack-token"

# Define database connection
db_conn <- dbConnect(
  drv = "pgsql",
  dbname = DB_NAME,
  host = DB_HOST,
  user = DB_USER,
  password = DB_PASSWORD
)

# Define pipeline status query
pipeline_status_query <- "SELECT * FROM pipeline_status WHERE status = 'failed'"

# Define notification function
notify_devops <- function(pipeline_name, error_message) {
  payload <- list(
    text = paste0("Pipeline ", pipeline_name, " failed: ", error_message)
  )
  payload_json <- toJSON(payload)
  
  httr::POST(
    url = "https://slack.com/api/chat.postMessage",
    body = payload_json,
    httr::add_headers(
      "Authorization" = paste0("Bearer ", NOTIF_TOKEN),
      "Content-Type" = "application/json"
    )
  )
}

# Define main function
main <- function() {
  # Fetch failed pipeline status
  pipe_status <- dbGetQuery(db_conn, pipeline_status_query)
  
  # Iterate over failed pipelines and send notifications
  for (row in 1:nrow(pipe_status)) {
    pipeline_name <- pipe_status[row, "pipeline_name"]
    error_message <- pipe_status[row, "error_message"]
    notify_devops(pipeline_name, error_message)
  }
  
  # Close database connection
  dbDisconnect(db_conn)
}

# Run the main function
main()