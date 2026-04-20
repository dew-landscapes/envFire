
#' Make last fire layer from fire history polygons
#'
#' @param fires_lyr sf object containing fire polygons.
#' @param date_field Character. Name of field indicating the fire start dates. 
#' @param parallel Logical. Run in parallel?
#' @param prop_cores Numeric. Proportion of available cores to use if `parallel` == TRUE. 
#' @param max_cores Integer. Maximum number of available cores to use if `parallel` == TRUE. 
#' 
#' @return sf object with polygons representing the last fire in an area. 

make_last_fire <- function(
    fires_lyr
    , date_field = "ignition_date"
    , parallel = FALSE
    , prop_cores = 0.5
    , max_cores = 124
) {
  
  # fires polygons ----
  fires <- fires_lyr |>
    dplyr::mutate(!!rlang::ensym(date_field) := lubridate::as_date(!!rlang::ensym(date_field)))
  
  # identify overlapping ----
  overlaps <- #sf::st_intersects(fires, fires) |> 
    sf::st_relate(fires, fires, pattern = "2********") |> 
    igraph::graph_from_adj_list() |> 
    igraph::components() %>% 
    .$membership
  
  fires2 <- fires |> 
    dplyr::mutate(clust = overlaps) |> 
    dplyr::add_count(clust)
  
  # filter overlapping ----
  # i.e. only those that overlap, as don't need to calc last fire for non-overlapping
  clusts <- fires2 |> 
    dplyr::filter(n > 1) |> 
    dplyr::arrange(desc(n)) |> 
    dplyr::select(-n) |> 
    tidyr::nest(data = -c(clust))
  
  # parallel setup ----
  if(parallel) {
    
    future::plan(future::multisession
                 , workers = min(envFunc::use_cores(prop_max_detected = prop_cores, absolute_max = max_cores)
                                 , nrow(clusts)
                 )
    ) # error using envFunc::use_cores with default 128 for absolute_max: 'Cannot create 128 parallel PSOCK nodes. Each node needs one connection, but there are only 124 connections left out of the maximum 128 available on this R installation. To increase this limit in R (>= 4.4.0), use command-line option '--max-connections=N' when launching R.', so use max given by error of 124. 
    
  } else {
    
    future::plan(future::sequential)
    
  }
  
  # reduce to last fire within overlapping clusters ----
  clust_results <- clusts$data |> 
    furrr::future_map(\(x) {
      
      # fires vector ----
      # i.e. vector of fires (dates) within cluster
      fires_vec <- x |>
        sf::st_drop_geometry() |>
        tibble::as_tibble() |> 
        dplyr::distinct(!!rlang::ensym(date_field)) |> 
        dplyr::arrange(desc(!!rlang::ensym(date_field))) |> 
        dplyr::pull()
      
      # reduce to last fire ----
      result <- purrr::reduce(fires_vec, .init = x, \(acc, nxt) {
        
        # this fire ----
        this_fire <- acc |> 
          dplyr::filter(!!rlang::ensym(date_field) == nxt) %>% 
          dplyr::group_by(across(c(-attr(., "sf_column")))) |>
          dplyr::summarise() |>
          dplyr::ungroup() |> 
          sf::st_make_valid()
        
        # older fires ----
        if(nrow(this_fire) > 0) {
          
          older_fires <- acc |> 
            dplyr::filter(!!rlang::ensym(date_field) < nxt) |> 
            sf::st_join(this_fire %>% 
                          dplyr::select(attr(., "sf_column")) |> # remove duplicated cols
                          dplyr::mutate(overlap = 1)
            ) |> 
            sf::st_make_valid() %>%
            {if(!"overlap" %in% names(.)) dplyr::mutate(., overlap = NA) else .}
          
        } else {
          
          older_fires <- acc |> 
            dplyr::filter(!!rlang::ensym(date_field) < nxt) |> 
            dplyr::mutate(overlap = NA)
          
        }
        
        # older overlap ----
        older_overlap <- older_fires |> 
          dplyr::filter(!is.na(overlap))
        
        # older no overlap ----
        older_no_overlap <- older_fires |> 
          dplyr::filter(is.na(overlap))
        
        # younger fires ----
        younger_fires <- acc |> 
          dplyr::filter(!!rlang::ensym(date_field) > nxt)
        
        # erase older overlapping & combine all ----
        if(nrow(older_overlap) > 0) {
          
          res1 <- tryCatch(
            {
              
              older_overlap |> 
                rmapshaper::ms_erase(this_fire, remove_slivers = TRUE) # produced 'Error: Not compatible with STRSXP: [type=list].' for one set of polygons in suburban Melbourne which looked like they had alignment/drawing errors. But, st_erase/st_difference works.
              
            },
            error = function(e) {
              
              older_overlap |> 
                sf::st_difference(this_fire)
              
            }
          )
          
          res <- res1 |> 
            sf::st_make_valid() |> 
            dplyr::bind_rows(older_no_overlap
                             , this_fire
                             , younger_fires
            )
          
        } else {
          
          res <- older_no_overlap |> 
            dplyr::bind_rows(this_fire
                             , younger_fires
            )
          
        }
        
      }
      )
      
    }
    , .progress = TRUE
    , .options = furrr::furrr_options(seed = TRUE)
    ) |> 
    dplyr::bind_rows() |> 
    ## add non-overlapping polygons ----
  dplyr::bind_rows(fires2 |> 
                     dplyr::filter(n == 1) |> 
                     dplyr::select(-n)
  ) |> 
    dplyr::arrange(desc(!!rlang::ensym(date_field))) |> 
    dplyr::select(-c(clust, overlap))
  
  return(clust_results)
  
}
