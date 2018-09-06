#' @importFrom odbc dbConnect
connect_rs <- function() {
  rs <- odbc::dbConnect(RPostgres::Postgres(),
                  "user" = Sys.getenv("RSUSER"),
                  "password" = Sys.getenv("RSPASSWORD"),
                  "dbname" = Sys.getenv("RSDB"),
                  "port"=Sys.getenv("RSPORT"),
                  "host" = Sys.getenv("RSHOST"))
}
