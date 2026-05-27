# ============================================================================
# Lazy-evaluation recipe implementations for FREQ, HMLC, and OSNA.
#
# These variants accept Arrow Datasets, arrow_dplyr_query objects, or DuckDB
# tbl_lazy objects (via dplyr/dbplyr) and push as much computation as possible
# into the underlying query engine before collecting into memory.
#
# The pipeline for each recipe is:
#   1. Enrich timestamp columns lazily (mutate — no collect).
#   2. Aggregate to user-level statistics lazily, then COLLECT (small table).
#   3. Apply user-level filters in memory and obtain valid user set.
#   4. semi_join the lazy frame to valid users, then aggregate to
#      (user × location) level lazily, then COLLECT (small table).
#   5. Apply location-level filters, scoring, and home extraction in memory.
#
# The two collects happen on already-aggregated tables (one row per user, then
# one row per user×location), which are always small relative to the raw data.
# ============================================================================


# ----------------------------------------------------------------------------
# Internal shared helpers
# ----------------------------------------------------------------------------

#' Enrich a lazy frame with temporal columns derived from the timestamp
#'
#' Returns the frame with new columns: year, month, day, wday, hour, day_id.
#' day_id = year*10000 + month*100 + day, used as a unique integer per
#' calendar day (avoids format() which is not translatable by all engines).
#'
#' @keywords internal
.enrich_lazy <- function(df, timestamp_expr) {
  df %>%
    dplyr::mutate(
      year   = lubridate::year(!!timestamp_expr),
      month  = lubridate::month(!!timestamp_expr),
      day    = lubridate::day(!!timestamp_expr),
      wday   = lubridate::wday(!!timestamp_expr),
      hour   = lubridate::hour(!!timestamp_expr),
      day_id = lubridate::year(!!timestamp_expr)  * 10000L +
               lubridate::month(!!timestamp_expr) *   100L +
               lubridate::day(!!timestamp_expr)
    )
}


#' Collect per-user summary statistics from a lazy frame
#'
#' Returns an in-memory tibble with one row per user containing n_points and
#' n_locs (distinct locations).  When \code{count_expr} is non-NULL the
#' dataset is pre-aggregated and n_points is computed as \code{sum(count)};
#' otherwise every row counts as one record.
#'
#' @keywords internal
.user_stats_lazy <- function(df, user_expr, location_expr, count_expr = NULL) {
  # Branch in R before the query is built — avoids dbplyr trying to translate
  # the R-level is.null() check into SQL.
  if (is.null(count_expr)) {
    df %>%
      dplyr::group_by(!!user_expr) %>%
      dplyr::summarise(
        n_points = dplyr::n(),
        n_locs   = dplyr::n_distinct(!!location_expr),
        .groups  = "drop"
      ) %>%
      dplyr::collect()
  } else {
    df %>%
      dplyr::group_by(!!user_expr) %>%
      dplyr::summarise(
        n_points = sum(!!count_expr, na.rm = TRUE),
        n_locs   = dplyr::n_distinct(!!location_expr),
        .groups  = "drop"
      ) %>%
      dplyr::collect()
  }
}


#' Filter an in-memory user-stats tibble: optionally remove top-N% most active
#' users, then apply minimum n_points / n_locs thresholds.
#'
#' @keywords internal
.filter_users_lazy <- function(df_user_stats,
                               threshold_n_points, threshold_n_locs,
                               rm_topNpct_user, topNpct) {
  if (rm_topNpct_user) {
    n_total  <- nrow(df_user_stats)
    cutoff_i <- max(1L, round(n_total * topNpct / 100))
    # The cutoff_i-th most active user's n_points — everyone above is removed.
    cutoff_pts <- df_user_stats %>%
      dplyr::arrange(dplyr::desc(n_points)) %>%
      dplyr::slice(cutoff_i) %>%
      dplyr::pull(n_points)
    df_user_stats <- df_user_stats %>%
      dplyr::filter(n_points < cutoff_pts)
    message(paste(emo::ji("bust_in_silhouette"),
                  "Removed top", topNpct, "% active users."))
  }

  df_user_stats %>%
    dplyr::filter(n_points > threshold_n_points,
                  n_locs   > threshold_n_locs)
}


#' Extract home location(s) from a flat (user x location) scored tibble.
#'
#' @param df       In-memory tibble with one row per user x location.
#' @param user_expr  rlang symbol for the user column.
#' @param location_expr rlang symbol for the location column.
#' @param score_col  String name of the column to rank by (descending).
#' @param show_n_loc Number of top locations to return per user.
#' @param keep_score Whether to keep all score columns in output.
#'
#' @keywords internal
.extract_home_flat <- function(df, user_expr, location_expr,
                               score_col, show_n_loc, keep_score) {
  score_sym <- rlang::sym(score_col)

  df_ranked <- df %>%
    dplyr::group_by(!!user_expr) %>%
    dplyr::arrange(dplyr::desc(!!score_sym), .by_group = TRUE)

  if (show_n_loc == 1L) {
    result <- df_ranked %>%
      dplyr::slice_head(n = 1L) %>%
      dplyr::ungroup() %>%
      dplyr::rename(home = !!location_expr)
  } else {
    result <- df_ranked %>%
      dplyr::slice_head(n = show_n_loc) %>%
      dplyr::summarise(home = paste(!!location_expr, collapse = "; "),
                       .groups = "drop")
  }

  n_users <- dplyr::n_distinct(result[[rlang::as_string(user_expr)]])
  message(paste(emo::ji("tada"),
                "Congratulations!! You have found", n_users,
                "users' potential home(s)."))

  if (keep_score) result else dplyr::select(result, !!user_expr, home)
}


# ----------------------------------------------------------------------------
# recipe_FREQ_lazy
# ----------------------------------------------------------------------------

#' Lazy-evaluation variant of the FREQ recipe
#'
#' Identifies home locations by visit frequency. Runs entirely as a lazy
#' query until after the (user x location) aggregation.
#'
#' @param df          A lazy frame (Arrow Dataset / DuckDB tbl_lazy).
#' @param user        Name of the user-id column.
#' @param timestamp   Name of the POSIXct timestamp column.
#' @param location    Name of the location-id column.
#' @param show_n_loc  Number of top locations to return per user.
#' @param keep_score  Whether to keep the n_points_loc score column.
#' @param use_default_threshold  Whether to use built-in thresholds.
#' @param rm_topNpct_user  Whether to remove the top 1% most active users.
#'
#' @importFrom rlang sym
#' @importFrom emo ji
#' @importFrom lubridate year month day wday hour
#' @importFrom dplyr mutate group_by summarise filter semi_join collect
#'   arrange desc slice_head rename select n n_distinct ungroup
#' @keywords internal
recipe_FREQ_lazy <- function(df, user = "u_id", timestamp = "created_at",
                             location = "loc_id", show_n_loc = 1L,
                             keep_score = FALSE, use_default_threshold = TRUE,
                             rm_topNpct_user = FALSE, count = NULL) {

  # --- thresholds (identical defaults to the nested recipe) -----------------
  if (use_default_threshold) {
    topNpct              <- 1
    threshold_n_points   <- 10
    threshold_n_locs     <- 10
    threshold_n_points_loc <- 10
  } else {
    topNpct <- readline(
      "How many percentage of top active users to remove (default = 1)? ") %>%
      as.integer()
    threshold_n_points <- readline(
      "Minimum total data points per user (default = 10)? ") %>% as.integer()
    threshold_n_locs <- readline(
      "Minimum unique locations per user (default = 10)? ") %>% as.integer()
    threshold_n_points_loc <- readline(
      "Minimum data points per user per location (default = 10)? ") %>%
      as.integer()
  }

  user_expr      <- rlang::sym(user)
  location_expr  <- rlang::sym(location)
  timestamp_expr <- rlang::sym(timestamp)
  count_expr     <- if (!is.null(count)) rlang::sym(count) else NULL

  message(paste(emo::ji("zap"),
                "FREQ (lazy path): enriching timestamp columns..."))
  start_time <- Sys.time()

  # Step 1 — enrich lazily (no collect) --------------------------------------
  df_enriched <- .enrich_lazy(df, timestamp_expr)

  # Step 2 — user-level stats → collect (O(users) rows) ---------------------
  message(paste(emo::ji("hammer_and_wrench"),
                "Aggregating user-level statistics..."))
  df_user_stats <- .user_stats_lazy(df_enriched, user_expr, location_expr,
                                    count_expr = count_expr)

  # Step 3 — filter users in memory ------------------------------------------
  df_users_valid <- .filter_users_lazy(
    df_user_stats, threshold_n_points, threshold_n_locs,
    rm_topNpct_user, topNpct)
  n_users_valid <- nrow(df_users_valid)
  message(paste(emo::ji("bust_in_silhouette"),
                n_users_valid, "users passed user-level filters."))

  # Step 4 — filter to valid users, aggregate to (user, loc) → collect -------
  # The is.null() branch is resolved in R before the query is built to avoid
  # dbplyr serialising it as a SQL CASE WHEN expression.
  valid_user_ids <- dplyr::pull(df_users_valid, !!user_expr)
  message(paste(emo::ji("hammer_and_wrench"),
                "Aggregating location-level statistics..."))
  df_loc_stats <- if (is.null(count_expr)) {
    df_enriched %>%
      dplyr::filter(!!user_expr %in% valid_user_ids) %>%
      dplyr::group_by(!!user_expr, !!location_expr) %>%
      dplyr::summarise(n_points_loc = dplyr::n(), .groups = "drop") %>%
      dplyr::collect()
  } else {
    df_enriched %>%
      dplyr::filter(!!user_expr %in% valid_user_ids) %>%
      dplyr::group_by(!!user_expr, !!location_expr) %>%
      dplyr::summarise(n_points_loc = sum(!!count_expr, na.rm = TRUE), .groups = "drop") %>%
      dplyr::collect()
  }

  # Step 5 — filter locations in memory --------------------------------------
  df_loc_filtered <- df_loc_stats %>%
    dplyr::filter(n_points_loc > threshold_n_points_loc)

  time_taken <- round(difftime(Sys.time(), start_time, units = "secs"), 3)
  message(paste(emo::ji("white_check_mark"),
                "Lazy aggregation complete in", time_taken, "secs."))

  # Step 6 — extract home(s) -------------------------------------------------
  .extract_home_flat(df_loc_filtered, user_expr, location_expr,
                     score_col  = "n_points_loc",
                     show_n_loc = show_n_loc,
                     keep_score = keep_score)
}


# ----------------------------------------------------------------------------
# recipe_HMLC_lazy
# ----------------------------------------------------------------------------

#' Lazy-evaluation variant of the HMLC recipe
#'
#' Computes the full HMLC multi-criteria score (visit frequency, temporal
#' spread, rest-time / weekend proportions) as a single grouped aggregation
#' that runs inside the Arrow / DuckDB engine.
#'
#' @inheritParams recipe_FREQ_lazy
#' @param keep_original_vars  Kept for API compatibility; ignored in the flat
#'   path (only score columns and the home identifier are returned).
#'
#' @importFrom rlang sym as_string
#' @importFrom emo ji
#' @importFrom lubridate year month day wday hour
#' @importFrom dplyr mutate group_by summarise filter semi_join collect
#'   arrange desc slice_head rename select n n_distinct ungroup if_else
#' @keywords internal
recipe_HMLC_lazy <- function(df, user = "u_id", timestamp = "created_at",
                             location = "loc_id", show_n_loc = 1L,
                             keep_original_vars = FALSE, keep_score = FALSE,
                             use_default_threshold = TRUE,
                             rm_topNpct_user = FALSE, count = NULL) {

  # --- thresholds -----------------------------------------------------------
  if (use_default_threshold) {
    topNpct                 <- 1
    threshold_n_points      <- 10
    threshold_n_locs        <- 10
    threshold_n_points_loc  <- 10
    threshold_n_hours_loc   <- 10
    threshold_n_days_loc    <- 10
    threshold_period_loc    <- 10
    w_n_points_loc  <- 0.1
    w_n_hours_loc   <- 0.1
    w_n_days_loc    <- 0.1
    w_n_wdays_loc   <- 0.1
    w_n_months_loc  <- 0.1
    w_period_loc    <- 0.1
    w_weekend       <- 0.1
    w_rest          <- 0.2
    w_weekend_am    <- 0.1
  } else {
    topNpct <- readline(
      "How many percentage of top active users to remove (default = 1)? ") %>%
      as.integer()
    threshold_n_points <- readline(
      "Minimum total data points per user (default = 10)? ") %>% as.integer()
    threshold_n_locs <- readline(
      "Minimum unique locations per user (default = 10)? ") %>% as.integer()
    threshold_n_points_loc <- readline(
      "Minimum data points per user per location (default = 10)? ") %>%
      as.integer()
    threshold_n_hours_loc <- readline(
      "Minimum unique hours per user per location (default = 10)? ") %>%
      as.integer()
    threshold_n_days_loc <- readline(
      "Minimum unique days per user per location (default = 10)? ") %>%
      as.integer()
    threshold_period_loc <- readline(
      "Minimum active period in days per location (default = 10)? ") %>%
      as.integer()
    w_n_points_loc  <- readline("Weight for n_points_loc  (default = 0.1)? ") %>% as.numeric()
    w_n_hours_loc   <- readline("Weight for n_hours_loc   (default = 0.1)? ") %>% as.numeric()
    w_n_days_loc    <- readline("Weight for n_days_loc    (default = 0.1)? ") %>% as.numeric()
    w_n_wdays_loc   <- readline("Weight for n_wdays_loc   (default = 0.1)? ") %>% as.numeric()
    w_n_months_loc  <- readline("Weight for n_months_loc  (default = 0.1)? ") %>% as.numeric()
    w_period_loc    <- readline("Weight for period_loc    (default = 0.1)? ") %>% as.numeric()
    w_weekend       <- readline("Weight for weekend prop  (default = 0.1)? ") %>% as.numeric()
    w_rest          <- readline("Weight for rest prop     (default = 0.2)? ") %>% as.numeric()
    w_weekend_am    <- readline("Weight for weekend_am    (default = 0.1)? ") %>% as.numeric()
  }

  user_expr      <- rlang::sym(user)
  location_expr  <- rlang::sym(location)
  timestamp_expr <- rlang::sym(timestamp)
  count_expr     <- if (!is.null(count)) rlang::sym(count) else NULL

  message(paste(emo::ji("zap"),
                "HMLC (lazy path): enriching timestamp columns..."))
  start_time <- Sys.time()

  # Step 1 — enrich lazily ---------------------------------------------------
  df_enriched <- .enrich_lazy(df, timestamp_expr)

  # Step 2 — user-level stats → collect --------------------------------------
  message(paste(emo::ji("hammer_and_wrench"),
                "Aggregating user-level statistics..."))
  df_user_stats <- .user_stats_lazy(df_enriched, user_expr, location_expr,
                                    count_expr = count_expr)

  # Step 3 — filter users in memory ------------------------------------------
  df_users_valid <- .filter_users_lazy(
    df_user_stats, threshold_n_points, threshold_n_locs,
    rm_topNpct_user, topNpct)
  message(paste(emo::ji("bust_in_silhouette"),
                nrow(df_users_valid), "users passed user-level filters."))

  # Step 4 — single-pass (user, loc) aggregation → collect -------------------
  # All HMLC features are computed here.
  # When count is provided (pre-aggregated rows), n_points_loc = sum(count)
  # and proportions are count-weighted: sum(condition * count) / sum(count).
  # When count is NULL every row = 1 record, so sum(1) = n() and the weighted
  # proportion reduces to the unweighted mean — identical to the original.
  message(paste(emo::ji("hammer_and_wrench"),
                "Aggregating location-level statistics (lazy)..."))
  valid_user_ids <- dplyr::pull(df_users_valid, !!user_expr)
  df_loc_stats <- if (is.null(count_expr)) {
    df_enriched %>%
      dplyr::filter(!!user_expr %in% valid_user_ids) %>%
      dplyr::group_by(!!user_expr, !!location_expr) %>%
      dplyr::summarise(
        n_points_loc = dplyr::n(),
        n_hours_loc  = dplyr::n_distinct(hour),
        n_days_loc   = dplyr::n_distinct(day_id),
        n_wdays_loc  = dplyr::n_distinct(wday),
        n_months_loc = dplyr::n_distinct(month),
        min_ts       = min(!!timestamp_expr, na.rm = TRUE),
        max_ts       = max(!!timestamp_expr, na.rm = TRUE),
        prop_weekend = mean(dplyr::if_else(wday %in% c(1L, 7L), 1.0, 0.0)),
        prop_rest    = mean(dplyr::if_else(hour < 9L | hour > 18L, 1.0, 0.0)),
        prop_wk_am   = mean(dplyr::if_else(
          hour >= 6L & hour <= 12L & wday %in% c(1L, 7L), 1.0, 0.0)),
        .groups = "drop"
      ) %>%
      dplyr::collect()
  } else {
    df_enriched %>%
      dplyr::filter(!!user_expr %in% valid_user_ids) %>%
      dplyr::group_by(!!user_expr, !!location_expr) %>%
      dplyr::summarise(
        n_points_loc = sum(!!count_expr, na.rm = TRUE),
        n_hours_loc  = dplyr::n_distinct(hour),
        n_days_loc   = dplyr::n_distinct(day_id),
        n_wdays_loc  = dplyr::n_distinct(wday),
        n_months_loc = dplyr::n_distinct(month),
        min_ts       = min(!!timestamp_expr, na.rm = TRUE),
        max_ts       = max(!!timestamp_expr, na.rm = TRUE),
        prop_weekend = sum(dplyr::if_else(wday %in% c(1L, 7L), !!count_expr, 0.0),
                          na.rm = TRUE) / sum(!!count_expr, na.rm = TRUE),
        prop_rest    = sum(dplyr::if_else(hour < 9L | hour > 18L, !!count_expr, 0.0),
                          na.rm = TRUE) / sum(!!count_expr, na.rm = TRUE),
        prop_wk_am   = sum(dplyr::if_else(
                          hour >= 6L & hour <= 12L & wday %in% c(1L, 7L),
                          !!count_expr, 0.0), na.rm = TRUE) /
                       sum(!!count_expr, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::collect()
  } %>%
    dplyr::mutate(
      period_loc = as.numeric(difftime(max_ts, min_ts, units = "days"))
    )

  # Step 5 — filter locations in memory -------------------------------------
  df_loc_filtered <- df_loc_stats %>%
    dplyr::filter(
      n_points_loc > threshold_n_points_loc,
      n_hours_loc  > threshold_n_hours_loc,
      n_days_loc   > threshold_n_days_loc,
      period_loc   > threshold_period_loc
    )

  if (nrow(df_loc_filtered) == 0) {
    stop(paste(emo::ji("bomb"),
               "No locations survived the location-level filters.",
               "Try lowering your thresholds with use_default_threshold = FALSE."))
  }

  # Step 6 — score (per-user normalization, then sum) ------------------------
  # max() denominators are per-user (group_by user first), matching the
  # original score_nested which nests by user before normalising.
  df_scored <- df_loc_filtered %>%
    dplyr::group_by(!!user_expr) %>%
    dplyr::mutate(
      s_n_points_loc = w_n_points_loc * (n_points_loc / max(n_points_loc)),
      s_n_hours_loc  = w_n_hours_loc  * (n_hours_loc  / 24),
      s_n_days_loc   = w_n_days_loc   * (n_days_loc   / max(n_days_loc)),
      s_n_wdays_loc  = w_n_wdays_loc  * (n_wdays_loc  / 7),
      s_n_months_loc = w_n_months_loc * (n_months_loc / 12),
      s_period_loc   = w_period_loc   * (period_loc   / max(period_loc)),
      s_weekend      = w_weekend      * prop_weekend,
      s_rest         = w_rest         * prop_rest,
      s_weekend_am   = w_weekend_am   * prop_wk_am
    ) %>%
    dplyr::mutate(
      score = s_n_points_loc + s_n_hours_loc + s_n_days_loc +
              s_n_wdays_loc  + s_n_months_loc + s_period_loc +
              s_weekend + s_rest + s_weekend_am
    ) %>%
    dplyr::ungroup()

  time_taken <- round(difftime(Sys.time(), start_time, units = "secs"), 3)
  message(paste(emo::ji("white_check_mark"),
                "Lazy aggregation complete in", time_taken, "secs."))

  # Step 7 — extract home(s) -------------------------------------------------
  .extract_home_flat(df_scored, user_expr, location_expr,
                     score_col  = "score",
                     show_n_loc = show_n_loc,
                     keep_score = keep_score)
}


# ----------------------------------------------------------------------------
# recipe_OSNA_lazy
# ----------------------------------------------------------------------------

#' Lazy-evaluation variant of the OSNA recipe
#'
#' Applies the Online Social Network Activity algorithm. Weekends and active-
#' time records are excluded before scoring, and the Rest / Leisure weighting
#' is computed with conditional aggregation (replacing pivot_wider), keeping
#' the entire operation lazy until the (user x location) collect.
#'
#' @inheritParams recipe_FREQ_lazy
#'
#' @importFrom rlang sym as_string
#' @importFrom emo ji
#' @importFrom lubridate year month day wday hour
#' @importFrom dplyr mutate group_by summarise filter semi_join collect
#'   arrange desc slice_head rename select n n_distinct ungroup if_else
#'   case_when
#' @keywords internal
recipe_OSNA_lazy <- function(df, user = "u_id", timestamp = "created_at",
                             location = "loc_id", show_n_loc = 1L,
                             keep_score = FALSE, use_default_threshold = TRUE,
                             rm_topNpct_user = FALSE, count = NULL) {

  # --- thresholds -----------------------------------------------------------
  if (use_default_threshold) {
    topNpct          <- 1
    threshold_n_locs <- 3
  } else {
    topNpct <- readline(
      "How many percentage of top active users to remove (default = 1)? ") %>%
      as.integer()
    threshold_n_locs <- readline(
      "Minimum unique locations per user (default = 3)? ") %>% as.integer()
  }

  # Weights from the original OSNA implementation
  weight_rest    <- mean(c(0.744, 0.735, 0.737))
  weight_leisure <- mean(c(0.362, 0.357, 0.354))

  user_expr      <- rlang::sym(user)
  location_expr  <- rlang::sym(location)
  timestamp_expr <- rlang::sym(timestamp)
  count_expr     <- if (!is.null(count)) rlang::sym(count) else NULL

  message(paste(emo::ji("zap"),
                "OSNA (lazy path): enriching timestamp columns..."))
  start_time <- Sys.time()

  # Step 1 — enrich lazily ---------------------------------------------------
  df_enriched <- .enrich_lazy(df, timestamp_expr)

  # Step 2 — user-level stats from ALL data → collect ------------------------
  message(paste(emo::ji("hammer_and_wrench"),
                "Aggregating user-level statistics..."))
  df_user_stats <- df_enriched %>%
    dplyr::group_by(!!user_expr) %>%
  df_user_stats <- if (is.null(count_expr)) {
    df_enriched %>%
      dplyr::group_by(!!user_expr) %>%
      dplyr::summarise(
        n_points = dplyr::n(),
        n_locs   = dplyr::n_distinct(!!location_expr),
        .groups  = "drop"
      ) %>%
      dplyr::collect()
  } else {
    df_enriched %>%
      dplyr::group_by(!!user_expr) %>%
      dplyr::summarise(
        n_points = sum(!!count_expr, na.rm = TRUE),
        n_locs   = dplyr::n_distinct(!!location_expr),
        .groups  = "drop"
      ) %>%
      dplyr::collect()
  }

  # Step 3 — remove top-N% most active users, then filter by n_locs ----------
  if (rm_topNpct_user) {
    n_total   <- nrow(df_user_stats)
    cutoff_i  <- max(1L, round(n_total * topNpct / 100))
    cutoff_pts <- df_user_stats %>%
      dplyr::arrange(dplyr::desc(n_points)) %>%
      dplyr::slice(cutoff_i) %>%
      dplyr::pull(n_points)
    df_user_stats <- df_user_stats %>%
      dplyr::filter(n_points < cutoff_pts)
    message(paste(emo::ji("bust_in_silhouette"),
                  "Removed top", topNpct, "% active users."))
  }
  df_users_valid <- df_user_stats %>%
    dplyr::filter(n_locs > threshold_n_locs)
  message(paste(emo::ji("bust_in_silhouette"),
                nrow(df_users_valid), "users passed user-level filters."))

  # Step 4 — OSNA scoring aggregation, entirely lazy -------------------------
  # When count is provided each row represents count raw records, so the score
  # contribution per row is weight * count rather than weight * 1.
  valid_user_ids <- dplyr::pull(df_users_valid, !!user_expr)
  message(paste(emo::ji("hammer_and_wrench"),
                "Computing OSNA location scores (lazy)..."))
  df_loc_scored <- df_enriched %>%
    dplyr::filter(!!user_expr %in% valid_user_ids) %>%
    # Remove weekends (wday 1 = Sunday, 7 = Saturday)
    dplyr::filter(!wday %in% c(1L, 7L)) %>%
    # Classify each row's time into Rest / Active / Leisure
    dplyr::mutate(
      timeframe = dplyr::case_when(
        hour >= 2L & hour <  8L  ~ "Rest",
        hour >= 8L & hour <  19L ~ "Active",
        TRUE                      ~ "Leisure"
      )
    ) %>%
    # Retain only Rest and Leisure rows
    dplyr::filter(timeframe != "Active") %>%
    dplyr::group_by(!!user_expr, !!location_expr) %>%
    { if (is.null(count_expr))
        dplyr::summarise(.,
          score = sum(
            dplyr::if_else(timeframe == "Rest",    weight_rest,    0.0) +
            dplyr::if_else(timeframe == "Leisure", weight_leisure, 0.0)
          ),
          .groups = "drop")
      else
        dplyr::summarise(.,
          score = sum(
            (dplyr::if_else(timeframe == "Rest",    weight_rest,    0.0) +
             dplyr::if_else(timeframe == "Leisure", weight_leisure, 0.0)) *
            !!count_expr,
            na.rm = TRUE),
          .groups = "drop")
    } %>%
    dplyr::collect()

  time_taken <- round(difftime(Sys.time(), start_time, units = "secs"), 3)
  message(paste(emo::ji("white_check_mark"),
                "Lazy aggregation complete in", time_taken, "secs."))

  # Step 5 — extract home(s) -------------------------------------------------
  .extract_home_flat(df_loc_scored, user_expr, location_expr,
                     score_col  = "score",
                     show_n_loc = show_n_loc,
                     keep_score = keep_score)
}
