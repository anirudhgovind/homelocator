check_column_exists <- function(data, colname, type) {
  if (inherits(data, "Dataset") || inherits(data, "tbl_lazy")) {
    if (!(colname %in% names(data))) {
      stop(paste0(type, " column does not exist in dataset!"))
    }
  } else if (is.data.frame(data)) {
    if (!rlang::has_name(data, colname)) {
      stop(paste0(type, " column does not exist in dataset!"))
    }
  } else {
    stop("Unsupported data type.")
  }
}

#' Check whether a data object is a lazy frame (Arrow Dataset or lazy DB table)
#' @keywords internal
is_lazy_frame <- function(df) {
  inherits(df, "Dataset") ||
    inherits(df, "arrow_dplyr_query") ||
    inherits(df, "tbl_lazy")
}

#' Collect a lazy frame into memory; return regular data frames unchanged
#' @keywords internal
collect_if_lazy <- function(df) {
  if (is_lazy_frame(df)) dplyr::collect(df) else df
}
