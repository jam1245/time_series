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

setwd("C:/Users/e394102/Documents/GitHub/time_series/R")
source("hive_connection.R")

df <- snaps %>%
  #select(`Employee ID`) %>% 
  select(`TTOC Descr Current`, `Month End Date`, `Headcount`, `Hire Count`, `Term Count`, `Alt Dept Descr`, `Job Level`, `Job Discipline`, 
         `Job Discipline Descr Current`, `Direct Indirect`, `Job Function Descr`, `Job Function`, `Full Part Time`, 
         `Casual to Full/Part - Req Count`, `Transfer In - Req Count`, `Work Loc City`, `Work Loc State`) %>% 
  as_tibble() %>% 
  clean_headers()

dbDisconnect(conn)
rm(conn)


library(remotes)
remotes::install_github("business-science/modeltime.gluonts")
devtools::install_github("business-science/modeltime.gluonts")
devtools::install_github("hadley/dplyr")

library(reticulate)
reticulate::conda_version()
#reticulate::py_module_available("gluonts")

library(dplyr)
my_gluonts_env_python_path <- reticulate::conda_list() %>%
  filter(name == "my_gluonts_env") %>%
  pull(python)

my_gluonts_env_python_path

Sys.setenv(GLUONTS_PYTHON = my_gluonts_env_python_path)

# verify it's been set 
Sys.getenv("GLUONTS_PYTHON")
#> "/Users/mdancho/Library/r-miniconda/envs/my_gluonts_env/bin/python"

######################################################################################################




#### Start Here ---------------------------------------
rms_tbl <- df %>% filter(full_part_time != "C") %>%
  ## LRP hiring definition 
  select(month_end_date, hire_count, casual_to_full_part_req_count, transfer_in_req_count) %>%
  mutate(sum = rowSums(.[2:4])) %>%
  group_by(month_end_date) %>% 
  summarise(hires = sum(sum) )




#rms_tbl <- df 


### Start the forecasting process here

################


rms_tbl %>%
  plot_time_series(month_end_date, hires)

### visualize 

rms_tbl %>%
  plot_acf_diagnostics(month_end_date, log(hires + 1), .lags = 1000)

#

fourier_periods <- c(12)
fourier_order <- 1
horizon <- 12



# Data Transformation
hires_trans_tbl <- rms_tbl %>%
  # Preprocess Target---- yeo johnson - review 
  mutate(hires_trans = log1p(hires)) %>% 
  # standarization -- centering and scaling which tranformas to mean = 0 and sd = 1 
  mutate(hires_trans = standardize_vec(hires_trans)) 





## create functions to hold the standard vec values -- for later use when transforming back  
standardize_vec_mean_val <- function(x) {
  
  if (!is.numeric(x)) rlang::abort("Non-numeric data detected. 'x' must be numeric.")
  
  {m <- round(mean(x, na.rm = T), digits = 6) }
  
  
  m
}

standardize_vec_sd_val <- function(x) {
  
  if (!is.numeric(x)) rlang::abort("Non-numeric data detected. 'x' must be numeric.")
  
  {s <- stats::sd(x, na.rm = T) }
  
  
  s
  
}


mean_val <- rms_tbl %>% mutate(hires = log(hires)) %>% summarise(mean = standardize_vec_mean_val(hires))
std_sd <- rms_tbl %>% mutate(hires = log(hires)) %>% summarise(sd = standardize_vec_sd_val(hires))






################################
horizon <- 12 # months 
lag_period <- c(12) 
rolling_periods <- c(12, 24, 36)


hires_prepared_full_tbl <- hires_trans_tbl %>%
  select(-hires) %>% 
  # add future window 
  bind_rows(
    future_frame(.data = ., .date_var = month_end_date, .length_out = horizon)
  ) %>%
  # add autocorrelated Lags 
  # TIP when adding lags - visualize them with the pivot longer step below 
  tk_augment_lags(hires_trans, .lags = lag_period) %>%
  
  # ADD FUTURE ROLLING LAGS features - use slidify for any rolling or window calculations to a datafame 
  tk_augment_slidify(
    # use lag 12 --- connected with the horizon / monthly values 
    .value = hires_trans_lag12, 
    .f = mean, .period = rolling_periods, # creates new columns - with a rolling avgs
    .align = "center", 
    .partial = TRUE 
  )  %>% # when you just add the above it's best to visualize using the below option and you'll notice that the lag stop at the value of forward periods
  
  ## use if adding events or other xregs 
  #   left_join(df_two, by = c("date_time" = "event_date")) %>%
  #    mutate(event_date = ifelse(is.na(event_date), 0, event_date)) %>%  ## adds zero or 1 where there's an event 
  ## tip format the column names into groups that start with the same code -- e.g. lag for lagged features which makes it easier to select them when modeling 
  #   rename(event_type = event_date) %>%
  rename_with(.cols = contains("lag"), .fn = ~ str_c("lag_", .)) 



###########################
#SEPARATE INTO MODELING & FORECAST DATA ----

hires_data_prepared_tbl <- hires_prepared_full_tbl %>% 
  # this will remove the NAs for the future years 
  filter(!is.na(hires_trans)) %>%
  # drop any lingering NAs in any other columns 
  replace(., is.na(.), 0)

hires_data_prepared_tbl
summary(hires_data_prepared_tbl) # check to ensure no NAs are in the lag columns

# here we have our future lag features but the hires_trans values are missing 
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


## create a training cleaned from outliers 
train_cleaned <- training(splits_hires) %>%
  #  group_by(pagePath) %>%
  # ts_clean_vec --- REMOVES OUTLIERS and replaces missing values 
  mutate(hires_trans = ts_clean_vec(hires_trans, period = 12)) # %>% # period = 7, working with daily series so set to weekly season
#ungroup()



###################################################
## Recipe Spec Base 


hires_recipe_spec_base <- recipe(hires_trans ~ ., data = training(splits_hires)) %>%
  # 
  # Time series signature --- adds a preprocessing step to generate the time series signature -- utilizes a date time column 
  step_timeseries_signature(month_end_date) %>%
  # Removing some features - columns that are not important for a monthly series  
  step_rm(matches("(iso)|(xts)|(hour)|(minute)|(second)|(am.pm)")) %>%  # regex () is used to create multi - regex search patterns 
  ## normally with larger features we should standardize 
  # Standardize - those features like time of year etc...
  # step normalize is equivalent to standardize vec function step range is equivalent to normalize_vec function 
  step_normalize(matches("(index.num)|(year)|(yday)")) %>%
  ## NEXT STEP One HOT ENCODING
  # will focus on anthing that is a text feature 
  step_dummy(all_nominal(), one_hot = TRUE) %>%
  ## Interaction 
  # this will add some additional features -- takes weeks two and multiplies it by wday.lbl - you'll see this in the glimpse below 
  step_interact(~ matches("week2") * matches("wday.lbl") ) %>%
  ## Last Step --- Add the fourier series features -- takes a date time --- we can add periods 
  step_fourier(month_end_date, period = c(12, 24, 36), K =2) #%>%
#  step_rm("month_end_date")


# Look at how your preprocessing steps are being applied 
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
  fit(training(splits_hires)) # %>% step_naomit()) ## trying to omit nas 



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

calibration_tbl %>% modeltime_forecast(new_data = testing(splits_hires), actual_data = hires_data_prepared_tbl) %>%
  plot_modeltime_forecast()

calibration_tbl %>% modeltime_accuracy() %>% arrange(rmse)


##########################################


# Model 1: auto_arima ----
model_fit_arima_no_boost <- arima_reg() %>%
  set_engine(engine = "auto_arima") %>%
  fit(hires_trans ~ month_end_date, data = training(splits_hires))


# Model 2: arima_boost ----
model_fit_arima_boosted <- arima_boost(
  min_n = 2,
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






model_spec_arima_boost <-  arima_boost(
  # typically we don't use seasonality here and let the xgboost take care of season 
  #seasonal_period = 12, 
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


###################

model_spec_prophet_boost <- prophet_boost(
  # prophet params
  #changepoint_num = 25, 
  #changepoint_range = 0.8, 
  # seasonality_yearly = TRUE, 
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


# * LIGHTGBM ----
#library(lightgbm)
#library(treesnip)
model_spec_light_gbm <- boost_tree(
  mode = "regression",
  min_n = 55, #
  tree_depth = 40, 
  learn_rate = 0.199, 
  loss_reduction = 0.115, 
  trees = 1000
) %>% set_engine("lightgbm")

set.seed(456)
wflw_fit_light_gbm_parms <- workflow() %>%
  add_model(model_spec_light_gbm) %>%
  add_recipe(hires_recipe_spec_base) %>%
  fit(training(splits_hires))



wflw_lightgbm_defaults <- workflow() %>%
  add_model(
    boost_tree(mode = "regression") %>%
      set_engine("lightgbm")
  ) %>%
  add_recipe(hires_recipe_spec_base) %>%
  fit(training(splits_hires))



###########################  View above models 
######################################################################################################

models_tbl <- modeltime_table(
  model_fit_arima_no_boost, 
  wflw_fit_arima_boost,
  wflw_fit_prophet_boost,
  model_fit_prophet,
  model_fit_lm,
  wflw_fit_rf, 
  wflw_fit_nnet, 
  wflw_fit_mars,
  wflw_fit_nnet_ar, 
  wflw_fit_xgboost
  # wflw_lightgbm_defaults, 
  #  wflw_fit_light_gbm_parms
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

# refit here 
refit_tbl <- calibration_tbl %>% 
  modeltime_refit(hires_data_prepared_tbl)

refit_tbl


#fcast_invert <- 
#  refit_tbl %>% 
# refit_ensemble_superlearner_tbl
#  modeltime_forecast(
#    new_data = hires_forecast_tbl, 
#hires_future_tbl, 
#    actual_data = hires_data_prepared_tbl
#  ) %>%

#  mutate(across(.value:.conf_hi, .fns = expm1))


### transform back 
fcast_invert <- refit_tbl %>%
  modeltime_forecast(new_data    = hires_forecast_tbl,
                     actual_data = hires_data_prepared_tbl) %>%
  
  # Invert Transformation
  mutate(across(.value:.conf_hi, .fns = ~ standardize_inv_vec(
    x    = .,
    mean = mean_val$mean,
    sd   = std_sd$sd
    # intervert out of log 
  ))) %>%
  mutate(across(.value:.conf_hi, .fns = expm1))



fcast_invert %>% 
  plot_modeltime_forecast()

fcast_invert %>% mutate( year = as.factor(year(.index) )) %>% 
  dplyr::mutate(.model_id = replace_na(.model_id, 0)) %>%
  filter(.model_id == 1 | .model_id == 0) %>%
  group_by(year) %>% 
  summarise(val = sum(.value))
#summarise_by_time(.index, .by = "year", .value)




######################################################################################################
library(modeltime.ensemble)
# ensemble step starts here 


######################  # GET TOP Performing Models ######

# get top models and/or diverstiy of models here for the ensemble 
model_id_selection <- calibration_tbl %>% 
  modeltime_accuracy() %>%
  arrange(rmse) %>%
  filter(.model_id == 6 | .model_id == 3 | .model_id == 10 | .model_id == 9) %>%  #| 
  #.model_id == 9 | .model_id == 3 | 
  #     .model_id == 1 
  #| .model_id == 4
  #  ) %>%
  #  dplyr::slice(1:3) %>%
  pull(.model_id)

model_id_selection

# do the actual subset or filter of the models
submodels_tbl <- calibration_tbl %>% 
  # filter(.model_id == 5 | .model_id == 2)
  filter(.model_id %in% model_id_selection)



#############################  SIMPLE ENSEMBLE #### NO STACKING 
ensemble_fit <- submodels_tbl %>%
  ensemble_average(type = "median")

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


################## Finalize 

# CODE HERE
refit_tbl <- ensemble_calibration_tbl %>% 
  modeltime_refit(hires_data_prepared_tbl)

refit_tbl


# invert here 
### transform back 
fcast_invert <- refit_tbl %>%
  modeltime_forecast(new_data    = hires_forecast_tbl,
                     actual_data = hires_data_prepared_tbl) %>%
  
  # Invert Transformation
  mutate(across(.value:.conf_hi, .fns = ~ standardize_inv_vec(
    x    = .,
    mean = mean_val$mean,
    sd   = std_sd$sd
    # invert out of log 
  ))) %>%
  mutate(across(.value:.conf_hi, .fns = expm1))




fcast_invert %>% 
  plot_modeltime_forecast()


fcast_invert %>% mutate( year = as.factor(year(.index) )) %>% 
  dplyr::mutate(.model_id = replace_na(.model_id, 0)) %>%
  filter(.model_id == 1 | .model_id == 0) %>%
  group_by(year) %>% 
  summarise(val = sum(.value), .conf_lo = sum(.conf_lo, na.rm = TRUE), .conf_hi = sum(.conf_hi, na.rm = TRUE))




fcast_invert %>% 
  dplyr::mutate(.model_id = replace_na(.model_id, 0)) %>%
  # just use the prophet xgboost model 
  #filter(.model_id == 2 | .model_id == 0) %>%
  write_rds("/mnt/Forecasts/Forecast_Data/rms_forecast.rds")

