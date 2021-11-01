library(tidyverse)
library(tidyquant)
library(gt)
library(gtExtras)
library(ggthemes)

tech_stocks <- tq_get(
  c("FB", "AMZN", "AAPL", "GOOG", "TWTR"),
  get = "stock.prices",
  from = "2018-01-01",
  "2021-10-05",
  complete_cases = TRUE
)

sum_df <- tech_stocks %>% 
  mutate(year = lubridate::year(date)) %>% 
  mutate(symbol = factor(
    symbol, levels = c("FB", "AMZN", "AAPL", "GOOG", "TWTR"),
    labels = c("facebook", "amazon", "apple", "google", "twitter"))
  ) %>% 
  filter(year == 2021) %>% 
  group_by(Symbol = symbol) %>% 
  summarise(
    N = length(close),
    `Max Close` = max(close), 
    `Min Close` = min(close),
    `Current Close` = last(close),
    `Sparkline '21`  = list(close),
    .groups = "drop"
  ) %>% 
  arrange(desc(`Current Close`))

sum_df

stock_tbl <- gt(sum_df) %>%
  gt_fa_column(Symbol, palette = c("#FF9900", "#DB4437", "#4267B2", "black", "#1DA1F2")) %>% 
  cols_label(Symbol = "") %>% 
  fmt_currency(columns = contains("Close")) %>% 
  gt_sparkline(`Sparkline '21`, same_limit = FALSE, line_color = "#40c5ff") %>% 
  theme_fivethirtyeight() %>% 
  tab_header(title = "Tech stocks in 2021") %>% 
  tab_source_note(
    md(
      paste0("**Data:** Yahoo Stocks via `{tidyquant}`\\\n",
             "**Table:** @thomas_mock via `{gt}`/`{gtExtras}`")
    )
  ) %>% 
  tab_footnote("Data as of 2021-10-04", locations = cells_column_labels(5))

stock_tbl