
renv::install("gumbelino/deliberr")

library(deliberr)
library(readr)
library(dplyr)

ITERATIONS <- 5
reasoning <- list(effort = "low")
temperature <- NA
model_id <- "google/gemini-3-pro-preview"
cc_cases <- read_csv("data/cc_cases.csv", show_col_types = F)
cc_roles <- read_csv("data/cc_roles.csv", show_col_types = F)

# test model_id
survey_info <- deliberr::surveys[deliberr::surveys$name == "energy_futures",]
role_info <- deliberr::roles[deliberr::roles$uid == "dis",]
llm_data <- get_dri_llm_response(model_id, survey_info, role_info = role_info,
                                 temperature = temperature,
                                 reasoning = reasoning,
                                 request_log_path = "data/manual_log.csv")

# calculate expected costs and time to generate full dataset
n_responses <- nrow(cc_cases) * nrow(cc_roles) * ITERATIONS
est_cost_usd <- n_responses * llm_data$est_cost_usd
est_time_h <- n_responses * llm_data$time_s / (60 * 60)
cat(model_id, paste(temperature, reasoning$effort, est_time_h, est_cost_usd, sep = "\t"))

## RUN THIS SECTION TO GENERATE FULL DATASET FOR A MODEL

LLM_DATA_FILE <- "data/llm_data_v2.csv"
ITERATIONS <- 5

model_id <- "anthropic/claude-3.7-sonnet:thinking"
temperature <- NA #2 #NA #2 #2 #2 # or 2
reasoning <- list(effort = "low")

llm_data <- list()

for (survey_name in c("ccps")) { #cc_cases$survey
  for (role_uid in c("vil", "csk")) { # cc_roles$uid
    
    survey_info <- deliberr::surveys[deliberr::surveys$name == survey_name,]
    role_info <- deliberr::roles[deliberr::roles$uid == role_uid,]
    
    cat(survey_name, role_uid)
    new_llm_data <- get_dri_llm_response(model_id, survey_info,
                                         role_info = role_info,
                                         temperature = temperature,
                                         reasoning = reasoning,
                                         n = 5, 
                                         request_log_path = "data/request_log.csv")
    
    llm_data[[length(llm_data)+1]] <- new_llm_data
  }
}

llm_data <- bind_rows(llm_data)

write_csv(llm_data, LLM_DATA_FILE, append = file.exists(LLM_DATA_FILE))

sum(llm_data$est_cost_usd)

## END SECTION
