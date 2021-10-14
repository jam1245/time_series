

suppressPackageStartupMessages({
  library(tidyverse)
  library(RJDBC)
  library(RPresto)
  library(janitor)
  library(dbplyr)
})


hive_connection <- function(host = "", 
                            table_name = "job_ew_snaps", 
                            schema = "hr",
                            port = 0000) {
  
  httr::set_config(
    httr::config(
      #  userpwd=paste0(Sys.getenv('dl_un'),':',Sys.getenv('dl_pw')),
      userpwd=paste0(rstudioapi::askForPassword("Database Username"),':',rstudioapi::askForPassword("Database Password")),
      ssl_verifypeer=0,
      cainfo='cacerts.pem'
    )
  )
  
  #Setting up data lake connection
  conn = RPresto::dbConnect(
    drv = Presto(), 
    catalog = 'hive', 
    schema = schema, 
    #  user = Sys.getenv('dl_un'), 
    #  password = Sys.getenv('dl_pw'),
    user = rstudioapi::askForPassword("Database Username"),
    password = rstudioapi::askForPassword("Database Password"),
    host = host, 
    port = port)
  
  
  tbl(conn, table_name) %>% head() %>% glimpse()
  
  database_callback <- tbl(conn, table_name)
  
  dbListTables(conn)
  
  dbDisconnect(conn)
  rm(conn)
  
  return(database_callback)
  
}
## example code ----

# how to use the function


# instantiate the hive connection --- it will default to ew_jop_snaps 
snaps <- hive_connection()


snaps_tbl_2017 <- snaps %>% 
  filter(`Business Area Code` == "A6500") %>%
  select(everything()) %>% 
  as_tibble() %>% 
  clean_names() %>% 
  filter(month_end_date == "2017-09-30")



# Query LM Careers ---- 

# select the lm carers table as it is in the data lake 

lm_careers_snaps <- hive_connection(table_name = "lmcareers_comprehensive_snaps")

lm_careers_query <- lm_careers_snaps %>% 
  ## this will allow you to select all column names by using select(everything())
  select(everything()) %>% 
  as_tibble() %>%
  clean_names() %>%
  filter(candidate_eoi_hr_status == "External Applied") %>% 
  filter(candidate_eoi_date >= "2015-10-26" & ttoc_desc == "ET-Engineering & Technology" | work_loc_code_rc == 1203) 



lm_careers_snaps %>% head() %>% as_tibble() %>% clean_names() %>% glimpse()

