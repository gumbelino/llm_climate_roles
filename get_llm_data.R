library(readr)
library(dplyr)

LLM_DATA_DIR <- "llm_data"
SURVEY_FILE <- "data/surveys_v5.xlsx"
EXEC_LOG_FILE <- paste(LLM_DATA_DIR, "exec_log.csv", sep = "/")
LLMS_FILE <- "private/pt2_llms.csv"
CASES_FILE <- "data/deliberative_cases.csv"
OUTPUT_DIR <- "pt2/data"

# define file types
file_types <- c("considerations", "policies", "reasons")

create_file_path <- function(provider, model, survey, file_type) {
  file.path(LLM_DATA_DIR, provider, model, survey, paste0(file_type, ".csv"))
}

# initialize an empty list to store the data frames
data_list <- list()

# get deliberative survey names
cases <- read_csv(CASES_FILE)
survey_names <- unique(cases$survey)

# get models
models <- read_csv(LLMS_FILE, show_col_types = FALSE) %>% filter(included)

# iterate over each survey
for (survey_name in survey_names) {
  
  # iterate over each row in the models data frame
  for (i in 1:nrow(models)) {
    
    provider <- models$provider[i]
    model <- models$model[i]

    # check if any file for the survey exists
    survey_path <- paste0(LLM_DATA_DIR, "/", provider, "/", model, "/", survey_name, "/")
    if (!any(file.exists(paste0(survey_path, file_types, ".csv")))) {
      next
    }
    
    # iterate over each file type
    for (file_type in file_types) {
      # create the file path
      file_path <- create_file_path(provider, model, survey_name, file_type)
      
      # check if the file exists
      if (!file.exists(file_path)) {
        break
      }
      
      # read the CSV file
      temp_data <- read_csv(file_path, show_col_types = FALSE)
      
      # skip file if file exists but has no data
      if (nrow(temp_data) == 0) {
        break
      }
      
      # select the relevant columns based on file type
      if (file_type == "considerations") {
        # initialize survey_data
        survey_data <- temp_data %>%
          rename_with(~ paste0("C", seq_along(.)),
                      starts_with("C", ignore.case = FALSE))
        
        # add column "survey" to meta data
        survey_data <- survey_data %>%
          mutate(survey = survey_name) %>%
          relocate(survey, .after = model)
        
        # ensure survey_data has columns up to C50
        # skip 9 rows of meta data
        for (j in (ncol(survey_data) - 9 + 1):50) {
          survey_data[[paste0("C", j)]] <- as.numeric(NA)
        }
        
        # go to next file type
        next
        
      } else if (file_type == "policies") {
        temp_data <- temp_data %>%
          select(cuid, starts_with("P", ignore.case = FALSE)) %>%
          rename_with(~ paste0("P", seq_along(.)),
                      starts_with("P", ignore.case = FALSE))
        
        # ensure temp_data has columns up to P10
        for (j in (ncol(temp_data)):10) {
          temp_data[[paste0("P", j)]] <- as.numeric(NA)
        }
        
      } else if (file_type == "reasons") {
        temp_data <- temp_data %>%
          select(cuid, reason) %>%
          rename(R = reason)
      }
      
      # merge the data frames by 'cuid' and keep all rows
      survey_data <- full_join(survey_data, temp_data, by = c("cuid"))
      
    }
    
    # add the survey_data to the list
    if (exists("survey_data")) {
      data_list[[length(data_list) + 1]] <- survey_data
      
      # remove the survey_data data frame to free up memory
      rm(survey_data)
    }
  }
}

# Combine all data frames in the list into a single data frame
llm_data <- bind_rows(data_list)

write_csv(llm_data, paste(OUTPUT_DIR, "llm_data.csv", sep = "/"))

source("pt2/remove_invalid_data.R")

llm_data <- remove_invalid_data(llm_data)

# filter out 5 iterations for each model/survey/prompt combination
llm_data_clean <- llm_data %>%
  group_by(model, survey, prompt_uid) %>%
  slice_head(n=5)

write_csv(llm_data_clean, paste(OUTPUT_DIR, "llm_data_clean.csv", sep = "/"))


