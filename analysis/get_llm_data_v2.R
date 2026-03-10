library(readr)
library(dplyr)

llm_data_v2 <- read_csv("data/llm_data_v2.csv")

# update data inconsistencies
llm_data_v2 <- llm_data_v2 %>%
  mutate(model = ifelse(is.na(model), "claude-sonnet-4.6", model),
         provider = ifelse(provider == "claude-sonnet-4.6", "anthropic", provider),
         
         # reasoning_effort is updated to NA when the model does not return 
         # reasonining content, but this is an issue with openrouter and due to
         # deliberr automated error catching
         # changing reasoning effort to *requested* reasoning effort
         # all reasoning efforts were set to "low"
         reasoning_effort = ifelse(is.na(temperature), "low", reasoning_effort))

# filter unwanted parameters
llm_data_v2 <- llm_data_v2 %>% 
  filter(temperature != 1 | is.na(temperature))


# check errors
errors <- llm_data_v2 %>% 
  filter(is_valid == F)

# errors by model
errors %>%
  group_by(model, invalid_reason) %>%
  summarise(n = n()) %>%
  arrange(-n)

# remove errors from dataset
llm_data_v2 <- llm_data_v2 %>% 
  filter(is_valid == T)

# get first 5 responses only
llm_data_v2 <- llm_data_v2 %>% 
  group_by(model, temperature, reasoning_effort, survey, role_uid) %>%
  slice_head(n = 5)

# only get complete datasets
llm_data_v2 <- llm_data_v2 %>% 
  group_by(model, temperature, reasoning_effort) %>%
  #summarize(n = n()) %>%
  filter(n() == 180) %>%
  # arrange(n) %>%
  ungroup()

# write clean data to file
write_csv(llm_data_v2, "data/llm_data_v2_clean.csv")
