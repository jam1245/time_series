
#' @description
#' Performs a forecast based on popular machine learning and sequential models
#'
#' @parm data A dataframe or tibble a date column named month_end_date and a value column named value
#'
#'
#' @details
#'
#' - Forecasting method uses future rolling lags at 12 months
#' - Forecasting method uses fourier series features -- updates in Dec 2022
#'
#' @examples
#' library(tidyverse)
#' library(tidymodels)
#' library(modeltime)
#' library(xgboost)
#' library(lubridate)
#' library(timetk)
#'
#' series_tbl <- m750 %>%
#'     rename(month_end_date = date) %>%
#'     select(month_end_date, value) %>%
#'     drop_na()
#'
#'
#' forecast_tbl <- series_tbl %>% forecast_forward(horizon = 48)
#'
#' forecast_tbl %>% plot_modeltime_forecast()
#'
#'
#' @export


forecast_forward <- function (data, horizon = 48 ) {
  
  value_trans_tbl <- data %>%
    # Preprocess Target - use log + 1 so that you don't have any issue with zero and Inf values
    dplyr::mutate(value_trans = log1p(value)) #%>%
  
  ################################
  horizon <- horizon # months
  lag_period <- c(horizon)
  rolling_periods <- c(12,24) # look at ACF and PCF for rolling lags
  
  
  value_prepared_full_tbl <- value_trans_tbl %>%
    dplyr::select(-value) %>%
    # add future window
    dplyr::bind_rows(
      timetk::future_frame(.data = ., .date_var = month_end_date, .length_out = horizon)
    ) %>%
    # add autocorrelated Lags
    # TIP when adding lags - visualize them with the pivot longer step below
    timetk::tk_augment_lags(value_trans, .lags = lag_period) %>%
    
    # ADD ROLLING LAG features - use slidify for any rolling or window calculations to a datafame
    timetk::tk_augment_slidify(
      # use lag 56 --- connected with your horizon
      .value = paste0("value_trans_lag", horizon),
      .f = mean, .period = rolling_periods, # creates new columns - with a rolling avg of 30, 60, 90
      .align = "center",
      .partial = TRUE
    )  %>% # when you just add the above it's best to visualize using the below option and you'll notice that the lag stop at the 30, 60, 90 day forward periods
    
    ## ADD EVENTS --- IF NEEDED---
    #   left_join(labs_prep_tbl, by = c("optin_time" = "event_date")) %>%
    #    mutate(event = ifelse(is.na(event), 0, event)) %>%  ## adds zero or 1 where there's an event
    ## tip format the column names into groups that start with the same code -- e.g. lag for lagged features which makes it easier to select them when modeling
    #   rename(lab_event = event) %>%
    dplyr::rename_with(.cols = contains("lag"), .fn = ~ str_c("lag_", .))
  
  
  
  ###########################
  # seperate into model and forecasting data
  
  # SEPARATE INTO MODELING & FORECAST DATA ----
  
  value_data_prepared_tbl <- value_prepared_full_tbl %>%
    # this will remove the NAs for the future years
    dplyr::filter(!is.na(value_trans)) %>%
    # drop any lingering NAs in any other columns
    replace(., is.na(.), 0)
  
  value_data_prepared_tbl
  summary(value_data_prepared_tbl) # check to ensure no NAs are in the lag columns
  
  # here we have our lag features but the value_trans values are missing
  value_forecast_tbl <- value_prepared_full_tbl %>%
    dplyr::filter(is.na(value_trans))
  
  value_forecast_tbl
  
  value_data_prepared_tbl %>% mutate_if(is.numeric, list(~na_if(., Inf))) %>%
    dplyr::mutate_if(is.numeric, list(~na_if(., -Inf)))
  
  
  # train test split
  splits_value <- value_data_prepared_tbl %>%
    timetk::time_series_split(assess = "12 months",
                              # initial = "1 year 1 months"
                              cumulative = TRUE)
  
  # visualize
  splits_value %>%
    timetk::tk_time_series_cv_plan() %>%
    timetk::plot_time_series_cv_plan(month_end_date, value_trans)
  
  
  ## create a training cleaned from outliers --
  train_cleaned <- rsample::training(splits_value) %>%
    #  group_by(pagePath) %>%
    # ts_clean_vec --- REMOVES OUTLIERS and replaces missing values
    mutate(value_trans = ts_clean_vec(value_trans, period = 12)) # %>% # period = 7, working with daily series so set to weekly season
  #ungroup()
  
  
  
  ###################################################
  ## Recipe Spec Base
  
  
  value_recipe_spec_base <- recipe(value_trans ~ ., data = rsample::training(splits_value)) %>%
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
  value_recipe_spec_base %>%
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
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  set.seed(123)
  wflw_fit_xgboost <- workflow() %>%
    add_model(model_spec_boost) %>%
    #add_recipe(value_recipe_spec_base) %>%
    # we have to remove the date var when using xgboost so we can update role or step_rm here
    #add_recipe(value_recipe_spec_base %>% update_role(month_end_date, new_role = "indicator")) %>%
    add_recipe(value_recipe_spec_base %>%
                 step_rm(month_end_date)
               #update_role(month_end_date, new_role = "indicator")
               
    ) %>%
    #fit(rsample::training(splits_value)) # %>% step_naomit()) ## trying to omit nas
    #fit(train_cleaned %>% select(-month_end_date))
    fit(rsample::training(splits_value))
  
  
  
  set.seed(123)
  wflw_fit_rf <- workflow() %>%
    add_model(model_spec_rf) %>%
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  set.seed(123)
  wflw_fit_nnet <- workflow() %>%
    add_model(model_spec_nnet) %>%
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  
  set.seed(123)
  wflw_fit_nnet_ar <- workflow() %>%
    add_model(model_spec_nnetar) %>%
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  
  
  
  #wflw_fit_nnet %>% pull_workflow_fit() %>% pluck("fit") %>% summary()
  
  
  # * Compare with Modeltime -----
  
  calibration_tbl <- modeltime_table(wflw_fit_rf,
                                     wflw_fit_nnet,
                                     #wflw_fit_nnet_ar,
                                     wflw_fit_xgboost
                                     #wflow_fit_svm_poly
                                     # model_fit_2_arima_sarimax
  ) %>%
    modeltime_calibrate(new_data = rsample::training(splits_value))
  
  # look at fit on rsample::training data
  calibration_tbl %>% modeltime_forecast(new_data = rsample::training(splits_value), actual_data = value_data_prepared_tbl) %>%
    plot_modeltime_forecast()
  
  #look at testing
  calibration_tbl %>% modeltime_forecast(new_data = testing(splits_value), actual_data = value_data_prepared_tbl) %>%
    plot_modeltime_forecast()
  
  # look at accuracy metrics
  
  calibration_tbl %>% modeltime_accuracy() %>% arrange(rmse)
  
  
  ##########################################
  
  
  # Model 1: auto_arima ----
  model_fit_arima_no_boost <- arima_reg() %>%
    set_engine(engine = "auto_arima") %>%
    fit(value_trans ~ month_end_date, data = rsample::training(splits_value))
  
  
  # Model 2: arima_boost ----
  model_fit_arima_boosted <- arima_boost(
    min_n = 2,
    tree_depth = 20,
    learn_rate = 0.015
  ) %>%
    set_engine(engine = "auto_arima_xgboost") %>%
    fit(value_trans ~ month_end_date +
          as.numeric(month_end_date) +
          factor(lubridate::month(month_end_date, label = TRUE), ordered = F),
        data = rsample::training(splits_value))
  #> frequency = 12 observations per 1 year
  # Model 3: ets ----
  model_fit_ets <- exp_smoothing() %>%
    set_engine(engine = "ets") %>%
    fit(value_trans ~ month_end_date, data = rsample::training(splits_value))
  #> frequency = 12 observations per 1 year
  
  
  # Model 4: prophet ----
  model_fit_prophet <- prophet_reg() %>%
    set_engine(engine = "prophet") %>%
    fit(value_trans ~ month_end_date, data = rsample::training(splits_value))
  #> Disabling weekly seasonality. Run prophet with weekly.seasonality=TRUE to override this.
  #> Disabling daily seasonality. Run prophet with daily.seasonality=TRUE to override this.
  
  model_fit_lm <- linear_reg() %>%
    set_engine("lm") %>%
    fit(value_trans ~ as.numeric(month_end_date) +
          factor(lubridate::month(month_end_date, label = TRUE), ordered = FALSE),
        data = rsample::training(splits_value))
  
  model_spec_mars <- mars(mode = "regression") %>%
    set_engine("earth")
  
  
  
  wflw_fit_mars <- workflow() %>%
    add_recipe(value_recipe_spec_base) %>%
    add_model(model_spec_mars) %>%
    fit(train_cleaned)
  
  
  
  model_spec_glmnet <- linear_reg(
    penalty = 0.05,
    mixture = 0.8
  ) %>%
    set_engine("glmnet")
  
  #workflow_fit_glmnet <- workflow() %>%
  #  add_recipe(value_recipe_spec_base) %>%
  #  add_model(model_spec_glmnet) %>%
  #  fit(value_trans ~ as.numeric(month_end_date) +
  #        factor(month(month_end_date, label = TRUE), ordered = FALSE),
  #      data = training(splits_value))
  #  fit(training(splits_value))
  
  
  
  
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
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  #wflw_fit_arima_reg <- workflow() %>%
  #  add_model(model_fit_arima_no_boost) %>%
  #  add_recipe(value_recipe_spec_base) %>%
  #  fit(training(splits_value))
  
  
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
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  
  ###########################  View above models
  ######################################################################################################
  
  models_tbl <- modeltime_table(
    model_fit_arima_no_boost,
    wflw_fit_arima_boost,
    wflw_fit_prophet_boost,
    model_fit_prophet,
    model_fit_lm,
    #workflow_fit_glmnet,
    wflw_fit_rf,
    wflw_fit_nnet,
    wflw_fit_mars,
    # wflw_fit_nnet_ar,
    wflw_fit_xgboost
  )
  
  calibration_tbl <- models_tbl %>%
    modeltime_calibrate(new_data = testing(splits_value))
  
  print('--------Accuracy Metrics on 12 Month Training Data ------ ')
  print(calibration_tbl %>% modeltime_accuracy() %>% arrange(rmse))
  
  calibration_tbl %>%
    modeltime_forecast(
      new_data    = testing(splits_value),
      actual_data = value_data_prepared_tbl
    ) %>%
    plot_modeltime_forecast()
  
  ################## Finalize
  refit_tbl <- calibration_tbl %>%
    modeltime_refit(value_data_prepared_tbl)
  
  refit_tbl
  
  
  # FINAL xform here
  fcast_invert <-
    refit_tbl %>%
    # refit_ensemble_superlearner_tbl %>%
    modeltime_forecast(
      new_data = value_forecast_tbl,
      #value_future_tbl,
      actual_data = value_data_prepared_tbl
    ) %>%
    # invert
    #  mutate(.value = log_interval_inv_vec(x = .value, limit_lower = 0, limit_upper = 100, offset = 1) )
    
    mutate(across(.value:.conf_hi, .fns = expm1))
  
  fcast_invert
  
  
}







#' @description
#' Performs an ensamble forecast based on popular machine learning and sequential models.  This function automatically combines
#' a arima xgboost with an prophet model out of the box.  You can set the model parameters here to select different moldels you'd
#' like to combine into one overall forecast.  The ensamble combination uses a median of each forecast. Recommend using the forecast_forward function
#' first to determine which models should be included in the ensamble forecast.  The models param relies on around eight different models which are
#' numbered and specified in the output of forecast_forward.  The ensamble defaults to combining two models and arima xgboost and an prophet xgboost
#'
#' @parm data A dataframe or tibble a date column named month_end_date and a value column named value
#'
#'
#' @details
#'
#' - Forecasting method uses future rolling lags at 12 months
#' - Forecasting method uses fourier series features
#'
#' @examples
#' library(tidyverse)
#' library(tidymodels)
#' library(modeltime)
#' library(xgboost)
#' library(lubridate)
#' library(timetk)
#' library(modeltime.ensemble)
#'
#' series_tbl <- m750 %>%
#'     rename(month_end_date = date) %>%
#'     select(month_end_date, value) %>%
#'     drop_na()
#'
#'
#' forecast_tbl <- series_tbl %>% ensamble_forecast(horizon = 48, models = c(2,3))
#'
#' forecast_tbl %>% plot_modeltime_forecast()
#'
#'
#'@export

ensamble_forecast <- function (data, horizon = 48, models = c(2,3)) {
  
  value_trans_tbl <- data %>%
    # Preprocess Target - use log + 1 so that you don't have any issue with zero and Inf values
    dplyr::mutate(value_trans = log1p(value)) #%>%
  
  ################################
  horizon <- horizon # months
  lag_period <- c(horizon)
  rolling_periods <- c(12,24) # look at ACF and PCF for rolling lags
  
  
  value_prepared_full_tbl <- value_trans_tbl %>%
    dplyr::select(-value) %>%
    # add future window
    dplyr::bind_rows(
      timetk::future_frame(.data = ., .date_var = month_end_date, .length_out = horizon)
    ) %>%
    # add autocorrelated Lags
    # TIP when adding lags - visualize them with the pivot longer step below
    timetk::tk_augment_lags(value_trans, .lags = lag_period) %>%
    
    # ADD ROLLING LAG features - use slidify for any rolling or window calculations to a datafame
    timetk::tk_augment_slidify(
      # use lag 56 --- connected with your horizon
      .value = paste0("value_trans_lag", horizon),
      .f = mean, .period = rolling_periods, # creates new columns - with a rolling avg of 30, 60, 90
      .align = "center",
      .partial = TRUE
    )  %>% # when you just add the above it's best to visualize using the below option and you'll notice that the lag stop at the 30, 60, 90 day forward periods
    
    ## ADD EVENTS
    #   left_join(learning_labs_prep_tbl, by = c("optin_time" = "event_date")) %>%
    #    mutate(event = ifelse(is.na(event), 0, event)) %>%  ## adds zero or 1 where there's an event
    ## tip format the column names into groups that start with the same code -- e.g. lag for lagged features which makes it easier to select them when modeling
    #   rename(lab_event = event) %>%
    dplyr::rename_with(.cols = contains("lag"), .fn = ~ str_c("lag_", .))
  
  
  
  ###########################
  # seperate into model and forecasting data
  
  # SEPARATE INTO MODELING & FORECAST DATA ----
  
  value_data_prepared_tbl <- value_prepared_full_tbl %>%
    # this will remove the NAs for the future years
    dplyr::filter(!is.na(value_trans)) %>%
    # drop any lingering NAs in any other columns
    replace(., is.na(.), 0)
  
  value_data_prepared_tbl
  summary(value_data_prepared_tbl) # check to ensure no NAs are in the lag columns
  
  # here we have our lag features but the value_trans values are missing
  value_forecast_tbl <- value_prepared_full_tbl %>%
    dplyr::filter(is.na(value_trans))
  
  value_forecast_tbl
  
  value_data_prepared_tbl %>% mutate_if(is.numeric, list(~na_if(., Inf))) %>%
    dplyr::mutate_if(is.numeric, list(~na_if(., -Inf)))
  
  
  # train test split
  splits_value <- value_data_prepared_tbl %>%
    timetk::time_series_split(assess = "12 months",
                              # initial = "1 year 1 months"
                              cumulative = TRUE)
  
  # visualize
  splits_value %>%
    timetk::tk_time_series_cv_plan() %>%
    timetk::plot_time_series_cv_plan(month_end_date, value_trans)
  
  
  ## create a training cleaned from outliers --
  train_cleaned <- rsample::training(splits_value) %>%
    #  group_by(pagePath) %>%
    # ts_clean_vec --- REMOVES OUTLIERS and replaces missing values
    mutate(value_trans = ts_clean_vec(value_trans, period = 12)) # %>% # period = 7, working with daily series so set to weekly season
  #ungroup()
  
  
  
  ###################################################
  ## Recipe Spec Base
  
  
  value_recipe_spec_base <- recipe(value_trans ~ ., data = rsample::training(splits_value)) %>%
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
  value_recipe_spec_base %>%
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
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  set.seed(123)
  wflw_fit_xgboost <- workflow() %>%
    add_model(model_spec_boost) %>%
    #add_recipe(value_recipe_spec_base) %>%
    # we have to remove the date var when using xgboost so we can update role or step_rm here
    #add_recipe(value_recipe_spec_base %>% update_role(month_end_date, new_role = "indicator")) %>%
    add_recipe(value_recipe_spec_base %>%
                 step_rm(month_end_date)
               #update_role(month_end_date, new_role = "indicator")
               
    ) %>%
    #fit(rsample::training(splits_value)) # %>% step_naomit()) ## trying to omit nas
    #fit(train_cleaned %>% select(-month_end_date))
    fit(rsample::training(splits_value))
  
  
  
  set.seed(123)
  wflw_fit_rf <- workflow() %>%
    add_model(model_spec_rf) %>%
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  set.seed(123)
  wflw_fit_nnet <- workflow() %>%
    add_model(model_spec_nnet) %>%
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  
  set.seed(123)
  wflw_fit_nnet_ar <- workflow() %>%
    add_model(model_spec_nnetar) %>%
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  
  
  
  #wflw_fit_nnet %>% pull_workflow_fit() %>% pluck("fit") %>% summary()
  
  
  # * Compare with Modeltime -----
  
  calibration_tbl <- modeltime_table(wflw_fit_rf,
                                     wflw_fit_nnet,
                                     #wflw_fit_nnet_ar,
                                     wflw_fit_xgboost
                                     #wflow_fit_svm_poly
                                     # model_fit_2_arima_sarimax
  ) %>%
    modeltime_calibrate(new_data = rsample::training(splits_value))
  
  # look at fit on training data
  calibration_tbl %>% modeltime_forecast(new_data = rsample::training(splits_value), actual_data = value_data_prepared_tbl) %>%
    plot_modeltime_forecast()
  
  #look at testing
  calibration_tbl %>% modeltime_forecast(new_data = testing(splits_value), actual_data = value_data_prepared_tbl) %>%
    plot_modeltime_forecast()
  
  # look at accuracy metrics
  calibration_tbl %>% modeltime_accuracy() %>% arrange(rmse)
  
  
  ##########################################
  
  
  # Model 1: auto_arima ----
  model_fit_arima_no_boost <- arima_reg() %>%
    set_engine(engine = "auto_arima") %>%
    fit(value_trans ~ month_end_date, data = rsample::training(splits_value))
  
  
  # Model 2: arima_boost ----
  model_fit_arima_boosted <- arima_boost(
    min_n = 2,
    tree_depth = 20,
    learn_rate = 0.015
  ) %>%
    set_engine(engine = "auto_arima_xgboost") %>%
    fit(value_trans ~ month_end_date +
          as.numeric(month_end_date) +
          factor(lubridate::month(month_end_date, label = TRUE), ordered = F),
        data = rsample::training(splits_value))
  #> frequency = 12 observations per 1 year
  # Model 3: ets ----
  model_fit_ets <- exp_smoothing() %>%
    set_engine(engine = "ets") %>%
    fit(value_trans ~ month_end_date, data = rsample::training(splits_value))
  #> frequency = 12 observations per 1 year
  
  
  # Model 4: prophet ----
  model_fit_prophet <- prophet_reg() %>%
    set_engine(engine = "prophet") %>%
    fit(value_trans ~ month_end_date, data = rsample::training(splits_value))
  #> Disabling weekly seasonality. Run prophet with weekly.seasonality=TRUE to override this.
  #> Disabling daily seasonality. Run prophet with daily.seasonality=TRUE to override this.
  
  model_fit_lm <- linear_reg() %>%
    set_engine("lm") %>%
    fit(value_trans ~ as.numeric(month_end_date) +
          factor(lubridate::month(month_end_date, label = TRUE), ordered = FALSE),
        data = rsample::training(splits_value))
  
  model_spec_mars <- mars(mode = "regression") %>%
    set_engine("earth")
  
  
  
  wflw_fit_mars <- workflow() %>%
    add_recipe(value_recipe_spec_base) %>%
    add_model(model_spec_mars) %>%
    fit(train_cleaned)
  
  
  
  model_spec_glmnet <- linear_reg(
    penalty = 0.05,
    mixture = 0.8
  ) %>%
    set_engine("glmnet")
  
  #workflow_fit_glmnet <- workflow() %>%
  #  add_recipe(value_recipe_spec_base) %>%
  #  add_model(model_spec_glmnet) %>%
  #  fit(value_trans ~ as.numeric(month_end_date) +
  #        factor(month(month_end_date, label = TRUE), ordered = FALSE),
  #      data = training(splits_value))
  #  fit(training(splits_value))
  
  
  
  
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
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  #wflw_fit_arima_reg <- workflow() %>%
  #  add_model(model_fit_arima_no_boost) %>%
  #  add_recipe(value_recipe_spec_base) %>%
  #  fit(training(splits_value))
  
  
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
    add_recipe(value_recipe_spec_base) %>%
    fit(rsample::training(splits_value))
  
  
  ###########################  View above models
  ######################################################################################################
  
  models_tbl <- modeltime_table(
    model_fit_arima_no_boost,
    wflw_fit_arima_boost,
    wflw_fit_prophet_boost,
    model_fit_prophet,
    model_fit_lm,
    #workflow_fit_glmnet,
    wflw_fit_rf,
    wflw_fit_nnet,
    wflw_fit_mars,
    # wflw_fit_nnet_ar,
    wflw_fit_xgboost
  )
  
  calibration_tbl <- models_tbl %>%
    modeltime_calibrate(new_data = testing(splits_value))
  
  calibration_tbl %>% modeltime_accuracy() %>% arrange(rmse)
  
  
  
  ######################  # GET TOP Performing Models ######
  # models of interest
  model_id_selection <- models ## set as an argument
  
  # do the actual subset or filter of the models
  submodels_tbl <- calibration_tbl %>%
    # filter(.model_id == 5 | .model_id == 2)
    filter(.model_id %in% model_id_selection)
  
  
  #############################  SIMPLE ENSEMBLE #### NO STACKING
  library(modeltime.ensemble)
  ensemble_fit <- submodels_tbl %>%
    ensemble_average(type = "median")
  
  ensemble_calibration_tbl <- modeltime_table(
    ensemble_fit
  ) %>%
    modeltime_calibrate(testing(splits_value))
  
  # Forecast vs Test Set
  
  
  ensemble_calibration_tbl %>% modeltime_accuracy()
  
  
  
  
  ################## Finalize ensemble
  
  # CODE HERE
  refit_tbl <- ensemble_calibration_tbl %>%
    modeltime_refit(value_data_prepared_tbl)
  
  refit_tbl
  
  
  # CODE HERE
  fcast_invert <-
    refit_tbl %>%
    # refit_ensemble_superlearner_tbl %>%
    modeltime_forecast(
      new_data = value_forecast_tbl,
      #value_future_tbl,
      actual_data = value_data_prepared_tbl
    ) %>%
    # invert
    #  mutate(.value = log_interval_inv_vec(x = .value, limit_lower = 0, limit_upper = 100, offset = 1) )
    
    mutate(across(.value:.conf_hi, .fns = expm1)) # transform back out of log
  
  # view forecast
  fcast_invert
  
  
  
}
