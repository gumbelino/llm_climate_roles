
# =========================
# 0. PACKAGES
# =========================
req_pkgs <- c(
  "tidyverse",
  "lme4",
  "lmerTest",
  "emmeans",
  "broom.mixed",
  "patchwork"
)

to_install <- req_pkgs[!req_pkgs %in% installed.packages()[, "Package"]]
if (length(to_install) > 0) install.packages(to_install)

library(tidyverse)
library(lme4)
library(lmerTest)
library(emmeans)
library(broom.mixed)
library(patchwork)

# =========================
# 1. LOAD DATA
# =========================
df <- read.csv("dri_alpha_data_ok.csv", stringsAsFactors = FALSE)

# =========================
# 2. CLEANING
# =========================
df <- df %>%
  mutate(
    model = factor(model),
    survey = factor(survey),
    role = factor(role),
    dri = as.numeric(dri),
    temperature = suppressWarnings(as.numeric(temperature)),
    reasoner = as.character(reasoner),
    reasoner_n = case_when(
      reasoner %in% c("1", "TRUE", "true", "yes", "Yes") ~ 1,
      reasoner %in% c("0", "FALSE", "false", "no", "No") ~ 0,
      TRUE ~ suppressWarnings(as.numeric(reasoner))
    )
  )

# =========================
# 3. CREATE ANALYSIS VARIABLES
# =========================

# H2: model configuration = model + operational condition
df_h2 <- df %>%
  mutate(
    condition = case_when(
      !is.na(temperature) ~ paste0("temp = ", temperature),
      is.na(temperature) & !is.na(reasoner_n) ~ paste0("reasoner = ", reasoner_n),
      TRUE ~ "default"
    ),
    model_configuration = paste(model, "|", condition),
    model_configuration = factor(model_configuration)
  )

# H3: only cases where temperature is observed
df_h3 <- df %>%
  filter(!is.na(dri), !is.na(temperature)) %>%
  mutate(
    model = factor(model),
    survey = factor(survey),
    temperature = factor(temperature)
  )

# H4: only models where reasoning can be toggled on/off
reasoning_capable_models <- df %>%
  group_by(model) %>%
  summarise(n_modes = n_distinct(reasoner_n, na.rm = TRUE), .groups = "drop") %>%
  filter(n_modes > 1) %>%
  pull(model)

df_h4 <- df %>%
  filter(model %in% reasoning_capable_models, !is.na(dri), !is.na(reasoner_n)) %>%
  mutate(
    model = factor(model),
    survey = factor(survey),
    reasoner_n = factor(reasoner_n, levels = c(0, 1))
  )

cat("\n=========================\n")
cat("DATA OVERVIEW\n")
cat("=========================\n")
cat("Full data rows: ", nrow(df), "\n")
cat("H2 rows: ", nrow(df_h2), "\n")
cat("H3 rows: ", nrow(df_h3), "\n")
cat("H4 rows: ", nrow(df_h4), "\n\n")

# =========================
# 4. COMMON THEME
# =========================
theme_paper <- theme_minimal(base_size = 13) +
  theme(
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    panel.grid.major.y = element_line(linewidth = 0.3, color = "grey85"),
    axis.title = element_text(face = "bold"),
    plot.title = element_text(face = "bold"),
    plot.subtitle = element_text(color = "grey30")
  )

dir.create("figures", showWarnings = FALSE)

# =========================
# 5. H2: MODEL CONFIGURATION EFFECT
# =========================

# Null model
m_h2_null <- lmer(
  dri ~ (1 | role) + (1 | survey),
  data = df_h2,
  REML = FALSE
)

# Full model
m_h2 <- lmer(
  dri ~ model_configuration + (1 | role) + (1 | survey),
  data = df_h2,
  REML = FALSE
)

cat("\n=========================\n")
cat("H2: MODEL CONFIGURATION EFFECT\n")
cat("=========================\n")
print(summary(m_h2))
print(anova(m_h2))
print(anova(m_h2_null, m_h2))

# Estimated marginal means
emm_h2 <- emmeans(m_h2, ~ model_configuration)
emm_h2_df <- as.data.frame(emm_h2)

p_h2 <- ggplot(
  emm_h2_df,
  aes(x = reorder(model_configuration, emmean), y = emmean, fill = emmean)
) +
  geom_col(width = 0.75, color = "grey20") +
  geom_errorbar(
    aes(ymin = lower.CL, ymax = upper.CL),
    width = 0.15,
    linewidth = 0.7
  ) +
  coord_flip() +
  scale_fill_gradient2(
    low = "#d73027",
    mid = "#fee08b",
    high = "#1a9850",
    midpoint = mean(emm_h2_df$emmean, na.rm = TRUE),
    name = "Predicted\nDRI"
  ) +
  labs(
    x = "Model configuration",
    y = "Estimated DRI",
    title = "H2: Deliberative Reason Index across model configurations",
    subtitle = "Mixed-effects model controlling for role and survey"
  ) +
  theme_paper +
  theme(
    panel.grid.major.y = element_blank(),
    legend.position = "right"
  )
p_h2
ggsave("figures/h2_model_configuration.png", p_h2, width = 11, height = 8, dpi = 300)

# =========================
# 6. H3: TEMPERATURE EFFECT
# =========================

m_h3 <- lmer(
  dri ~ temperature + (1 | model) + (1 | survey),
  data = df_h3
)

cat("\n=========================\n")
cat("H3: TEMPERATURE EFFECT\n")
cat("=========================\n")
print(summary(m_h3))
print(anova(m_h3))

# coefficient dataframe
coef_h3_df <- broom.mixed::tidy(
  m_h3,
  effects = "fixed",
  conf.int = TRUE
) %>%
  filter(term != "(Intercept)") %>%
  mutate(term_label = "Temperature 0/2")

# =========================
# 7. H4: REASONING EFFECT
# =========================

m_h4 <- lmer(
  dri ~ reasoner_n + (1 | model) + (1 | survey),
  data = df_h4
)

cat("\n=========================\n")
cat("H4: REASONING EFFECT\n")
cat("=========================\n")
print(summary(m_h4))
print(anova(m_h4))

coef_h4_df <- broom.mixed::tidy(
  m_h4,
  effects = "fixed",
  conf.int = TRUE
) %>%
  filter(term != "(Intercept)") %>%
  mutate(term_label = "Reasoning OFF/ON")

# =========================
# 8. SHARED COEFFICIENT AXIS FOR H3/H4
# =========================

xmin <- min(coef_h3_df$conf.low, coef_h4_df$conf.low, na.rm = TRUE)
xmax <- max(coef_h3_df$conf.high, coef_h4_df$conf.high, na.rm = TRUE)
x_limits <- c(xmin, xmax)

# -------------------------
# H3 coefficient plot
# -------------------------
p_h3_coef <- ggplot(coef_h3_df, aes(x = estimate, y = term_label)) +
  geom_vline(
    xintercept = 0,
    linetype = "dotted",
    color = "red3",
    linewidth = 1.2
  ) +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), height = 0.15, linewidth = 1) +
  geom_point(size = 3) +
  coord_cartesian(xlim = x_limits) +
  labs(
    x = "Coefficient estimate",
    y = NULL,
    title = "H3: Temperature effect",
    subtitle = "Mixed-model estimate with 95% CI"
  ) +
  theme_paper +
  theme(
    legend.position = "none",
    panel.grid.major.y = element_blank()
  )

# -------------------------
# H4 coefficient plot
# -------------------------
p_h4_coef <- ggplot(coef_h4_df, aes(x = estimate, y = term_label)) +
  geom_vline(
    xintercept = 0,
    linetype = "dotted",
    color = "red3",
    linewidth = 1.2
  ) +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), height = 0.15, linewidth = 1) +
  geom_point(size = 3) +
  coord_cartesian(xlim = x_limits) +
  labs(
    x = "Coefficient estimate",
    y = NULL,
    title = "H4: Reasoner effect",
    subtitle = "Mixed-model estimate with 95% CI"
  ) +
  theme_paper +
  theme(
    legend.position = "none",
    panel.grid.major.y = element_blank()
  )

p_h3_h4 <- p_h3_coef / p_h4_coef
p_h3_h4
ggsave("figures/h3_h4_coefficients.png", p_h3_h4, width = 10, height = 8, dpi = 300)

# =========================
# 9. OPTIONAL: WITHIN-MODEL SLOPE PLOTS
# =========================

# H3 slope plot
df_h3_plot <- df_h3 %>%
  group_by(model, temperature) %>%
  summarise(mean_dri = mean(dri, na.rm = TRUE), .groups = "drop")

p_h3_slope <- ggplot(
  df_h3_plot,
  aes(x = temperature, y = mean_dri, group = model, color = model)
) +
  geom_line(linewidth = 1, alpha = 0.8) +
  geom_point(size = 3) +
  labs(
    x = "Temperature",
    y = "Mean DRI",
    title = "Within-model change",
    subtitle = "Average DRI by temperature"
  ) +
  theme_paper

# H4 slope plot
df_h4_plot <- df_h4 %>%
  group_by(model, reasoner_n) %>%
  summarise(mean_dri = mean(dri, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    reasoner_n = factor(reasoner_n, levels = c(0, 1),
                        labels = c("Reasoning OFF", "Reasoning ON"))
  )

p_h4_slope <- ggplot(
  df_h4_plot,
  aes(x = reasoner_n, y = mean_dri, group = model, color = model)
) +
  geom_line(linewidth = 1, alpha = 0.8) +
  geom_point(size = 3) +
  labs(
    x = NULL,
    y = "Mean DRI",
    title = "Within-model change",
    subtitle = "Average DRI by reasoning mode"
  ) +
  theme_paper
p_h4_slope
p_h3_slope

(p_h4_coef/p_h4_slope)
((p_h3_coef/p_h3_slope))
ggsave("figures/h3_temperature_slope.png", p_h3_slope, width = 8, height = 5, dpi = 300)
ggsave("figures/h4_reasoner_slope.png", p_h4_slope, width = 8, height = 5, dpi = 300)

cat("\n=========================\n")
cat("DONE\n")
cat("=========================\n")
cat("Saved:\n")
cat("- figures/h2_model_configuration.png\n")
cat("- figures/h3_h4_coefficients.png\n")
cat("- figures/h3_temperature_slope.png\n")
cat("- figures/h4_reasoner_slope.png\n")