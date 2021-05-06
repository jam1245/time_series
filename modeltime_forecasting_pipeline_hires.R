library(tidyverse)
library(RJDBC)
library(RPresto)
library(feather)
library(modeltime)
library(timetk)
library(janitor)
library(dbplyr)
library(tidymodels)
library(lubridate)


## clean up column headers 
clean_headers <- function(data) {
  names(data) <- gsub(" ","_",names(data))
  names(data) <- gsub("-","",names(data))
  names(data) <- gsub("[()]", "", names(data))
  data <- janitor::clean_names(data)
  return(data)
  
} 




httr::set_config(
  httr::config(
    userpwd=paste0(Sys.getenv('dl_un'),':',Sys.getenv('dl_pw')),
    #   userpwd=paste0(rstudioapi::askForPassword("Database Username"),':',rstudioapi::askForPassword("Database Password")),
    ssl_verifypeer=0,
    cainfo='cacerts.pem'
  )
)

#Setting up data lake connection
conn = RPresto::dbConnect(
  drv = Presto(), 
  catalog = 'hive', 
  schema = 'hr', 
  user = Sys.getenv('dl_un'), 
  password = Sys.getenv('dl_pw'),
  #user = rstudioapi::askForPassword("Database Username"),
  #password = rstudioapi::askForPassword("Database Password"),
  host = 'host', 
  port = port)





## End(Not run)
## Not run: 
#First create a database connection with src_presto, then reference a tbl
#within that database
#my_tbl <- tbl(my_db, "job_rms_snaps")


#  head(10) %>%
#  show_query()
#  collect()

tbl(conn, "job_rms_snaps") %>% head()

snaps <- tbl(conn, "job_rms_snaps")
#snaps <- snaps %>% as_tibble()

#%>% 


df <- snaps %>%
  #select(`Employee ID`) %>% 
  select(`TTOC Descr Current`, `Month End Date`, `Headcount`, `Hire Count`, `Term Count`, `Alt Dept Descr`, `Job Level`, `Job Discipline`, 
         `Job Discipline Descr Current`, `Direct Indirect`, `Job Function Descr`, `Job Function`, `Full Part Time`, 
         `Casual to Full/Part - Req Count`, `Transfer In - Req Count`, `Work Loc City`, `Work Loc State`) %>% 
  as_tibble() %>% 
  clean_headers()

dbDisconnect(conn)
rm(conn)




######### C6 E&T ############

c6_et_tbl <- df_join %>% filter(full_part_time != "C") %>% 
  select(month_end_date, hire_count, casual_to_full_part_req_count, transfer_in_req_count, lob, ttoc_descr_current) %>%
  filter(lob == "C6ISR") %>% 
  filter(ttoc_descr_current == "ET-Engineering & Technology"  
         #  ttoc_descr_current == "OP-Operations" | 
         #   ttoc_descr_current == "FI-Finance & Business Ops" |
         #   ttoc_descr_current == "C6-C6ISR" 
  ) %>% 
  mutate(sum = rowSums(.[2:4])) %>%
  group_by(month_end_date) %>% 
  summarise(hires = sum(sum) ) #%>%
# filter out the early C6ISR values 
#  filter( month_end_date >= "2016-01-01")



## plot

c6_et_tbl %>%
  plot_time_series(month_end_date, hires)

### visualize 

c6_et_tbl %>%
  plot_acf_diagnostics(month_end_date, log(hires + 1), .lags = 1000)

## seasonal diagnostics 
c6_et_tbl %>%
  plot_seasonal_diagnostics(
    .date_var = month_end_date, 
    .value    = log(hires + 1),
    .feature_set = c("month.lbl", "quarter"),
    .geom        = "violin"
  )


c6_et_tbl %>%
  plot_stl_diagnostics(month_end_date, log(hires + 1)
                       #, .frequency = "1 month", .trend = "6 month"
  )

## here we see that month is pretty important 
c6_et_tbl %>% 
  plot_time_series_regression(
    .date_var = month_end_date, 
    log(hires +1) ~ as.numeric(month_end_date) +
      #   wday(month_end_date, label = TRUE) + 
      month(month_end_date, label = TRUE), 
    .show_summary = TRUE
  )

fourier_periods <- c(12)
fourier_order <- 1
horizon <- 12



# Data Transformation
hires_trans_tbl <- c6_et_tbl %>%
  
  # Preprocess Target - use log + 1 so that you don't have any issue with zero and Inf values 
  mutate(hires_trans = log1p(hires)) #%>% 

#log_interval_vec(hires, 
#                            limit_lower = 0, offset = 1
#                            )) 




################################
horizon <- 12 # months 
lag_period <- c(12) 
rolling_periods <- c(12,24) # look at ACF and PCF for rolling lags 


hires_prepared_full_tbl <- hires_trans_tbl %>%
  select(-hires) %>% 
  # add future window 
  bind_rows(
    future_frame(.data = ., .date_var = month_end_date, .length_out = horizon)
  ) %>%
  # add autocorrelated Lags 
  # TIP when adding lags - visualize them with the pivot longer step below 
  tk_augment_lags(hires_trans, .lags = lag_period) %>%
  
  # ADD ROLLING LAG features - use slidify for any rolling or window calculations to a datafame 
  tk_augment_slidify(
    # use lag 56 --- connected with your horizon
    .value = hires_trans_lag12, 
    .f = mean, .period = rolling_periods, # creates new columns - with a rolling avg of 30, 60, 90
    .align = "center", 
    .partial = TRUE 
  )  %>% # when you just add the above it's best to visualize using the below option and you'll notice that the lag stop at the 30, 60, 90 day forward periods
  
  ## ADD EVENTS 
  #   left_join(learning_labs_prep_tbl, by = c("optin_time" = "event_date")) %>%
  #    mutate(event = ifelse(is.na(event), 0, event)) %>%  ## adds zero or 1 where there's an event 
  ## tip format the column names into groups that start with the same code -- e.g. lag for lagged features which makes it easier to select them when modeling 
  #   rename(lab_event = event) %>%
  rename_with(.cols = contains("lag"), .fn = ~ str_c("lag_", .)) 



###########################
# seperate into model and forecasting data 

# SEPARATE INTO MODELING & FORECAST DATA ----

hires_data_prepared_tbl <- hires_prepared_full_tbl %>% 
  # this will remove the NAs for the future years 
  filter(!is.na(hires_trans)) %>%
  # drop any lingering NAs in any other columns 
  replace(., is.na(.), 0)

hires_data_prepared_tbl
summary(hires_data_prepared_tbl) # check to ensure no NAs are in the lag columns

# here we have our lag features but the hires_trans values are missing 
hires_forecast_tbl <- hires_prepared_full_tbl %>%
  filter(is.na(hires_trans))

hires_forecast_tbl





# * Create Future Data ----
#hires_future_tbl <- hires_trans_tbl%>%
#    future_frame(.date_var = month_end_date, .length_out = "12 months")  %>%
#  mutate(hires_trans = as.double(NA)) %>%
## taken from above 
#  tk_augment_fourier(month_end_date, .periods = fourier_periods)  

#hires_future_tbl

###### Train test split ###################################
hires_data_prepared_tbl %>% mutate_if(is.numeric, list(~na_if(., Inf))) %>% 
  mutate_if(is.numeric, list(~na_if(., -Inf)))


# train test split 
splits_hires <- hires_data_prepared_tbl %>%
  time_series_split(assess = "12 months", 
                    # initial = "1 year 1 months"
                    cumulative = TRUE)

# visualize 
splits_hires %>%
  tk_time_series_cv_plan() %>%
  plot_time_series_cv_plan(month_end_date, hires_trans)


## create a training cleaned from outliers -- 
train_cleaned <- training(splits_hires) %>%
  #  group_by(pagePath) %>%
  # ts_clean_vec --- REMOVES OUTLIERS and replaces missing values 
  mutate(hires_trans = ts_clean_vec(hires_trans, period = 12)) # %>% # period = 7, working with daily series so set to weekly season
#ungroup()



###################################################
## Recipe Spec Base 


hires_recipe_spec_base <- recipe(hires_trans ~ ., data = training(splits_hires)) %>%
  # 
  # Time series signature --- addsa a preprocessing step to generate the time series signature -- utilizes a date time column 
  step_timeseries_signature(month_end_date) %>%
  # Now we can remove features - columns 
  step_rm(matches("(iso)|(xts)|(hour)|(minute)|(second)|(am.pm)")) %>%  # regex () is used to create multi - regex search patterns 
  ## normally with larger features we should standardize 
  # Standardize - those features like time of year etc...
  # steop normalize is equivalent to standardize vec function step range is equivalent to normalize_vec function 
  step_normalize(matches("(index.num)|(year)|(yday)")) %>%
  ## NEXT STEP One HOT ENCODING
  # will focus on anthing that is a text feature 
  step_dummy(all_nominal(), one_hot = TRUE) %>%
  ## Interaction 
  # this will add some additional features -- takes weeks two and multiplies it by wday.lbl - you'll see this in the glimpse below 
  step_interact(~ matches("week2") * matches("wday.lbl") ) %>%
  ## Last Step --- Add the fourier series features -- takes a date time --- we can add periods 
  step_fourier(month_end_date, period = c(12,24), K =2)# %>%
 # step_rm("month_end_date")


# always a good idea to look at how your preprocessing steps are being applied 
hires_recipe_spec_base %>%
  # prepares the dataset - it's being trained 
  prep() %>%
  # juice returns the training data with the prepared recipe applied 
  juice() %>%
  glimpse()


############################################################################
# xgboost 
model_spec_boost <- boost_tree(
  mode = "regression", 
  mtry = 30, 
  trees = 1000, 
  min_n = 1, 
  tree_depth = 15, 
  learn_rate = 0.013, 
  loss_reduction = 0.1
)  %>% 
  set_engine("xgboost")

model_spec_rf <- rand_forest(
  mode = "regression", 
  mtry = 50, 
  trees = 1000, 
  min_n = 5
) %>%
  set_engine("randomForest")

model_spec_nnet <- mlp(
  mode = "regression",
  hidden_units = 10,
  penalty = 2, 
  epochs = 50
) %>%
  set_engine("nnet")



model_spec_svm_rbf <- svm_rbf(
  mode = "regression",
  cost = 1, 
  rbf_sigma = 0.01,
  margin = 0.1
) %>%
  set_engine("kernlab")


model_spec_nnetar <- nnetar_reg(
  non_seasonal_ar = 2,
  seasonal_ar     = 12,  # uses seasonal period which is auto detected in the series (weekly, monthly in our case)
  hidden_units    = 10,
  penalty         = 10,
  num_networks    = 10,
  epochs          = 50
) %>%
  set_engine("nnetar")


# spline 


# * Workflow ----

set.seed(123)
wflow_fit_svm_poly <- workflow() %>%
  add_model(model_spec_svm_rbf) %>%
  add_recipe(hires_recipe_spec_base) %>%
  fit(training(splits_hires))

set.seed(123)
wflw_fit_xgboost <- workflow() %>% 
  add_model(model_spec_boost) %>% 
  #add_recipe(hires_recipe_spec_base) %>% 
  # we have to remove the date var when using xgboost so we can update role or step_rm here 
  add_recipe(hires_recipe_spec_base %>% update_role(month_end_date, new_role = "indicator")) %>% 
  #fit(training(splits_hires)) # %>% step_naomit()) ## trying to omit nas 
  #fit(train_cleaned %>% select(-month_end_date))
  fit(training(splits_hires))



set.seed(123) 
wflw_fit_rf <- workflow() %>% 
  add_model(model_spec_rf) %>% 
  add_recipe(hires_recipe_spec_base) %>%
  fit(training(splits_hires))

set.seed(123)
wflw_fit_nnet <- workflow() %>% 
  add_model(model_spec_nnet) %>% 
  add_recipe(hires_recipe_spec_base) %>% 
  fit(training(splits_hires))


set.seed(123)
wflw_fit_nnet_ar <- workflow() %>% 
  add_model(model_spec_nnetar) %>% 
  add_recipe(hires_recipe_spec_base) %>% 
  fit(training(splits_hires))




wflw_fit_nnet %>% pull_workflow_fit() %>% pluck("fit") %>% summary()


# * Compare with Modeltime -----

calibration_tbl <- modeltime_table(wflw_fit_rf, 
                                   wflw_fit_nnet, 
                                   wflw_fit_nnet_ar, 
                                   wflw_fit_xgboost
                                   #wflow_fit_svm_poly
                                   # model_fit_2_arima_sarimax
) %>%
  modeltime_calibrate(new_data = training(splits_hires))

# look at fit on training data 
calibration_tbl %>% modeltime_forecast(new_data = training(splits_hires), actual_data = hires_data_prepared_tbl) %>%
  plot_modeltime_forecast()

#look at testing 
calibration_tbl %>% modeltime_forecast(new_data = testing(splits_hires), actual_data = hires_data_prepared_tbl) %>%
  plot_modeltime_forecast()

# look at accuracy metrics
calibration_tbl %>% modeltime_accuracy() %>% arrange(rmse)


##########################################


# Model 1: auto_arima ----
model_fit_arima_no_boost <- arima_reg() %>%
  set_engine(engine = "auto_arima") %>%
  fit(hires_trans ~ month_end_date, data = training(splits_hires))


# Model 2: arima_boost ----
model_fit_arima_boosted <- arima_boost(
  min_n = 2,
  tree_depth = 20,
  learn_rate = 0.015
) %>%
  set_engine(engine = "auto_arima_xgboost") %>%
  fit(hires_trans ~ month_end_date + 
        as.numeric(month_end_date) + 
        factor(month(month_end_date, label = TRUE), ordered = F),
      data = training(splits_hires))
#> frequency = 12 observations per 1 year
# Model 3: ets ----
model_fit_ets <- exp_smoothing() %>%
  set_engine(engine = "ets") %>%
  fit(hires_trans ~ month_end_date, data = training(splits_hires))
#> frequency = 12 observations per 1 year


# Model 4: prophet ----
model_fit_prophet <- prophet_reg() %>%
  set_engine(engine = "prophet") %>%
  fit(hires_trans ~ month_end_date, data = training(splits_hires))
#> Disabling weekly seasonality. Run prophet with weekly.seasonality=TRUE to override this.
#> Disabling daily seasonality. Run prophet with daily.seasonality=TRUE to override this.

model_fit_lm <- linear_reg() %>%
  set_engine("lm") %>%
  fit(hires_trans ~ as.numeric(month_end_date) + 
        factor(month(month_end_date, label = TRUE), ordered = FALSE),
      data = training(splits_hires))

model_spec_mars <- mars(mode = "regression") %>%
  set_engine("earth") 



wflw_fit_mars <- workflow() %>%
  add_recipe(hires_recipe_spec_base) %>%
  add_model(model_spec_mars) %>%
  fit(train_cleaned)



model_spec_glmnet <- linear_reg(
  penalty = 0.05, 
  mixture = 0.8
) %>%
  set_engine("glmnet")

workflow_fit_glmnet <- workflow() %>%
  add_recipe(hires_recipe_spec_base) %>%
  add_model(model_spec_glmnet) %>%
#  fit(hires_trans ~ as.numeric(month_end_date) + 
#        factor(month(month_end_date, label = TRUE), ordered = FALSE),
#      data = training(splits_hires))
  fit(training(splits_hires))




model_spec_arima_boost <-  arima_boost(
  # typically we don't use seasonality here and let the xgboost take care of season 
  seasonal_period = 12, 
  #  non_seasonal_ar = 2, 
  #  non_seasonal_differences =1, 
  #  non_seasonal_ma = 1, 
  #  seasonal_ar = 0, 
  #  seasonal_differences = 0, 
  #  seasonal_ma = 1, 
  
  mtry = 2,   ## note setting to zero shuts off xgboost effect --- by setting mtry = 0 
  min_n = 20, 
  tree_depth = 20, 
  learn_rate = 0.012, 
  loss_reduction = 0.15, 
  trees = 1000
  
) %>% 
  #set_engine("arima_xgboost")
  set_engine("auto_arima_xgboost")


set.seed(456)
wflw_fit_arima_boost <- workflow() %>%
  add_model(model_spec_arima_boost) %>%
  add_recipe(hires_recipe_spec_base) %>%
  fit(training(splits_hires))

wflw_fit_arima_reg <- workflow() %>% 
  add_model(model_fit_arima_no_boost) %>% 
  add_recipe(hires_recipe_spec_base) %>% 
  fit(training(splits_hires))


###################

model_spec_prophet_boost <- prophet_boost(
  # prophet params
  #changepoint_num = 25, 
  #changepoint_range = 0.8, 
  seasonality_yearly = TRUE, 
  #  seasonality_weekly = FALSE, 
  #  seasonality_daily = FALSE,
  
  #xgboost parmas
  # when set high it forces xgboost to shut down (foreces algorithum to not make any splits which essentially 
  #predicts the mean of the residuals) - reduced effect of xgboost to 
  #adding a constant to the prophet results that constand it the avg residuals 
  min_n = 55, #
  tree_depth = 25, 
  learn_rate = 0.199, ## note on above you can also shut off xgboost effect by setting mtry = 0 
  loss_reduction = 0.115, 
  trees = 1000
) %>%
  set_engine("prophet_xgboost") 

# Workflow
set.seed(456)
wflw_fit_prophet_boost <- workflow() %>%
  add_model(model_spec_prophet_boost) %>%
  add_recipe(hires_recipe_spec_base) %>%
  fit(training(splits_hires))


###########################  View above models 
######################################################################################################

models_tbl <- modeltime_table(
  model_fit_arima_no_boost,
 # wflw_fit_arima_boost,
  wflw_fit_prophet_boost,
  model_fit_prophet,
  model_fit_lm,
  #workflow_fit_glmnet, 
  wflw_fit_rf, 
  wflw_fit_nnet, 
  wflw_fit_mars,
  wflw_fit_nnet_ar, 
  wflw_fit_xgboost
)

calibration_tbl <- models_tbl %>%
  modeltime_calibrate(new_data = testing(splits_hires))

calibration_tbl %>% modeltime_accuracy() %>% arrange(rmse)

calibration_tbl %>%
  modeltime_forecast(
    new_data    = testing(splits_hires),
    actual_data = hires_data_prepared_tbl
  ) %>%
  plot_modeltime_forecast()

################## Finalize 

# 
refit_tbl <- calibration_tbl %>% 
  modeltime_refit(hires_data_prepared_tbl)

refit_tbl


# CODE HERE
fcast_invert <- 
  refit_tbl %>% 
  # refit_ensemble_superlearner_tbl %>%
  modeltime_forecast(
    new_data = hires_forecast_tbl, 
  #hires_future_tbl, 
    actual_data = hires_data_prepared_tbl
  ) %>%
# invert 
  #  mutate(.value = log_interval_inv_vec(x = .value, limit_lower = 0, limit_upper = 100, offset = 1) )
  
  mutate(across(.value:.conf_hi, .fns = expm1))

fcast_invert %>% 
  plot_modeltime_forecast()

## use this bit of code to see the annual prediction values compared to prior years 
fcast_invert %>% mutate( year = as.factor(year(.index) )) %>% 
  dplyr::mutate(.model_id = replace_na(.model_id, 0)) %>%
  filter(.model_id == 2 | .model_id == 0) %>%
  group_by(year) %>% 
  summarise(val = sum(.value)) 
 #summarise_by_time(.index, .by = "year", .value)


# fcast_invert %>% 
#  dplyr::mutate(.model_id = replace_na(.model_id, 0)) %>%
#  # just use the prophet xgboost model 
#  #filter(.model_id == 5) %>% 
#  filter(.model_id == 7 | .model_id == 0) %>%
#  write_rds("/mnt/Forecasts/Forecast_Data/iwss_forecast.rds")

#f <- read_rds("/mnt/Forecasts/Forecast_Data/iwss_forecast.rds")




######################  # GET TOP Performing Models ######

# do the actual subset or filter of the models
model_id_selection <- calibration_tbl %>% 
  modeltime_accuracy() %>%
  arrange(rmse) %>%
  # select models you want to ensemble 
  filter(.model_id == 4 | .model_id == 5) %>%
  #.model_id == 9 | .model_id == 3 | 

  #| .model_id == 4
  #  ) %>%
#  dplyr::slice(1:4) %>%
  pull(.model_id)

model_id_selection # models of interest

# do the actual subset or filter of the models
submodels_tbl <- calibration_tbl %>% 
  # filter(.model_id == 5 | .model_id == 2)
  filter(.model_id %in% model_id_selection)


#############################  SIMPLE ENSEMBLE #### NO STACKING 
library(modeltime.ensemble)
ensemble_fit <- submodels_tbl %>%
  ensemble_average(type = "mean")

ensemble_calibration_tbl <- modeltime_table(
  ensemble_fit
) %>%
  modeltime_calibrate(testing(splits_hires))

# Forecast vs Test Set

ensemble_calibration_tbl %>%
  modeltime_forecast(
    new_data    = testing(splits_hires),
    actual_data = hires_data_prepared_tbl
  ) %>%
  plot_modeltime_forecast()

ensemble_calibration_tbl %>% modeltime_accuracy() 




################## Finalize ensemble

# CODE HERE
refit_tbl <- ensemble_calibration_tbl %>% 
  modeltime_refit(hires_data_prepared_tbl)

refit_tbl


# CODE HERE
fcast_invert <- 
  refit_tbl %>% 
  # refit_ensemble_superlearner_tbl %>%
  modeltime_forecast(
    new_data = hires_forecast_tbl, 
    #hires_future_tbl, 
    actual_data = hires_data_prepared_tbl
  ) %>%
  # invert 
  #  mutate(.value = log_interval_inv_vec(x = .value, limit_lower = 0, limit_upper = 100, offset = 1) )
  
  mutate(across(.value:.conf_hi, .fns = expm1)) # transform back out of log 

# view forecast 
fcast_invert %>% 
  plot_modeltime_forecast()

# view annual forecast 
fcast_invert %>% mutate( year = as.factor(year(.index) )) %>% 
  dplyr::mutate(.model_id = replace_na(.model_id, 0)) %>%
  filter(.model_id == 1 | .model_id == 0) %>%
  group_by(year) %>% 
  summarise(val = sum(.value))
#summarise_by_time(.index, .by = "year", .value)



# save forecasts 
fcast_invert %>% 
  dplyr::mutate(.model_id = replace_na(.model_id, 0)) %>%
  # just use the prophet xgboost model 
  #filter(.model_id == 5) %>% 
  filter(.model_id == 1 | .model_id == 0) %>%
  write_rds("/mnt/Forecasts/Forecast_Data/c6_forecast.rds")

f <- read_rds("/mnt/Forecasts/Forecast_Data/c6_forecast.rds")

######################################################################################################


