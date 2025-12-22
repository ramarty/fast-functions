

# gBuffer ----------------------------------------------------------------------
gBuffer_chunks <- function(sdf,width,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf),by=chunk_size)
  
  gBuffer_i <- function(start, sdf, width, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf))
    sdf_buff_i <- gBuffer(sdf[start:end,],width=width, byid=T)
    print(paste0(start, " / ", nrow(sdf)))
    return(sdf_buff_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    sdf_buff <- pbmclapply(starts, gBuffer_i, sdf, width, chunk_size, mc.cores=mc.cores) %>% do.call(what="rbind")
  } else{
    sdf_buff <- lapply(starts, gBuffer_i, sdf, width, chunk_size) %>% do.call(what="rbind")
  }
  
  return(sdf_buff)
}

# st_buffer --------------------------------------------------------------------
st_buffer_chunks <- function(sdf,dist,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf),by=chunk_size)
  
  st_buffer_i <- function(start, sdf, dist, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf))
    sdf_buff_i <- st_buffer(sdf[start:end,],dist=dist)
    print(paste0(start, " / ", nrow(sdf)))
    return(sdf_buff_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    sdf_buff <- pbmclapply(starts, st_buffer_i, sdf, dist, chunk_size, mc.cores=mc.cores) %>% bind_rows()
  } else{
    sdf_buff <- lapply(starts, st_buffer_i, sdf, dist, chunk_size) %>% bind_rows()
  }
  
  return(sdf_buff)
}

# geo.buffer ----------------------------------------------------------------------
geo.buffer_chunks <- function(sdf, r, chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf),by=chunk_size)
  
  geo.buffer_i <- function(start, sdf, width, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf))
    sdf_buff_i <- geo.buffer(sdf[start:end,],r=r)
    print(paste0(start, " / ", nrow(sdf)))
    return(sdf_buff_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    sdf_buff <- pbmclapply(starts, geo.buffer_i, sdf, r, chunk_size, mc.cores=mc.cores) %>% do.call(what="rbind")
  } else{
    sdf_buff <- lapply(starts, geo.buffer_i, sdf, r, chunk_size) %>% do.call(what="rbind")
  }
  
  return(sdf_buff)
}

# gDistance ----------------------------------------------------------------------
gDistance_chunks <- function(sdf1,sdf2,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf1),by=chunk_size)
  
  gDistance_i <- function(start, sdf1, sdf2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf1))
    distances_i <- gDistance(sdf1[start:end,],sdf2, byid=T)
    print(paste0(start, " / ", nrow(sdf1)))
    return(distances_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    distances <- pbmclapply(starts, gDistance_i, sdf1, sdf2, chunk_size, mc.cores=mc.cores) %>% unlist %>% as.numeric
  } else{
    distances <- lapply(starts, gDistance_i, sdf1, sdf2, chunk_size) %>% unlist %>% as.numeric
  }
  
  return(distances)
}

# gIntersects ------------------------------------------------------------------
gIntersects_chunks <- function(sdf1,sdf2,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf1),by=chunk_size)
  
  gIntersects_i <- function(start, sdf1, sdf2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf1))
    distances_i <- gIntersects(sdf1[start:end,],sdf2, byid=T)
    print(paste0(start, " / ", nrow(sdf1)))
    return(distances_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    intersects_tf <- pbmclapply(starts, gIntersects_i, sdf1, sdf2, chunk_size, mc.cores=mc.cores) %>% unlist %>% as.vector()
  } else{
    intersects_tf <- lapply(starts, gIntersects_i, sdf1, sdf2, chunk_size) %>% unlist %>% as.vector()
  }
  
  return(intersects_tf)
}

# over ----------------------------------------------------------------------
over_chunks <- function(sdf1,sdf2,fn_type,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf1),by=chunk_size)
  
  over_i <- function(start, sdf1, sdf2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf1))
    
    if(fn_type %in% "sum") df_i <- sp::over(sdf1[start:end,], sdf2, fn=function(x) sum(x, na.rm=T))
    if(fn_type %in% "mean") df_i <- sp::over(sdf1[start:end,], sdf2, fn=function(x) mean(x, na.rm=T))
    if(fn_type %in% "median") df_i <- sp::over(sdf1[start:end,], sdf2, fn=function(x) median(x, na.rm=T))
    if(fn_type %in% "max") df_i <- sp::over(sdf1[start:end,], sdf2, fn=function(x) max(x, na.rm=T))
    if(fn_type %in% "none") df_i <- sp::over(sdf1[start:end,], sdf2)
    
    print(paste(start, "/", nrow(sdf1)))
    return(df_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    df <- pbmclapply(starts, over_i, sdf1, sdf2, chunk_size, mc.cores=mc.cores) %>% bind_rows
  } else{
    df <- lapply(starts, over_i, sdf1, sdf2, chunk_size) %>% bind_rows
  }
  
  return(df)
}

# raster::aggregate ------------------------------------------------------------
raster_aggregate_chunks <- function(sdf,chunk_size,final_aggregate,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf),by=chunk_size)
  
  sdf$id_agg <- 1
  
  aggregate_i <- function(start, sdf, chunk_size){
    print(paste0(start, " / ", nrow(sdf)))
    end <- min(start + chunk_size - 1, nrow(sdf))
    sdf_i <- raster::aggregate(sdf[start:end,], by="id_agg")
    
    return(sdf_i)
  } 
  
  if(mc.cores > 1){
    library(parallel)
    df <- pbmclapply(starts, aggregate_i, sdf, chunk_size, mc.cores=mc.cores) %>% do.call(what="rbind")
  } else{
    df <- lapply(starts, aggregate_i, sdf, chunk_size) %>% do.call(what="rbind")
  }
  
  if(final_aggregate) df <- raster::aggregate(df, by="id_agg")
  
  return(df)
}

# st_length --------------------------------------------------------------------
st_length_chunks <- function(sdf1,chunk_size,mc.cores=1){
  # Wrapper for st_length; to avoid clogging up memory, loops through in
  # chunks. ASSUMES sdf2 IS ONE ROW!
  
  starts <- seq(from=1,to=nrow(sdf1),by=chunk_size)
  
  st_lengths_i <- function(start, sdf1, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf1))
    lengths_i <- st_length(sdf1[start:end,]) %>% as.vector()
    print(start)
    return(lengths_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    lengths_all <- pbmclapply(starts, st_lengths_i, sdf1, chunk_size, mc.cores=mc.cores) %>% unlist %>% as.numeric
  } else{
    lengths_all <- lapply(starts, st_lengths_i, sdf1, chunk_size) %>% unlist %>% as.numeric
  }
  
  return(lengths_all)
}

# st_intersection ----------------------------------------------------------------------
st_intersection_chunks <- function(sf_1, sf_2, chunk_size, mc.cores=1){
  starts <- seq(from=1,to=nrow(sf_1),by=chunk_size)
  
  st_intersection_i <- function(start, sf_1, sf_2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sf_1))
    sf_1_i <- st_intersection(sf_1[start:end,],sf_2)
    print(paste0(start, " / ", nrow(sf_1)))
    return(sf_1_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    out <- pbmclapply(starts, st_intersection_i, sf_1, sf_2, chunk_size, mc.cores=mc.cores) %>% do.call(what="rbind")
  } else{
    out <- lapply(starts, st_intersection_i, sf_1, sf_2, chunk_size) %>% do.call(what="rbind")
  }
  
  return(out)
}

# st_intersection ----------------------------------------------------------------------
st_intersects_chunks <- function(sf_1, sf_2, chunk_size, mc.cores=1){
  starts <- seq(from=1,to=nrow(sf_1),by=chunk_size)
  
  st_intersects_i <- function(start, sf_1, sf_2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sf_1))
    sf_1_i <- st_intersects(sf_1[start:end,],sf_2,sparse=F) %>% as.vector()
    print(paste0(start, " / ", nrow(sf_1)))
    return(sf_1_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    out <- pbmclapply(starts, st_intersects_i, sf_1, sf_2, chunk_size, mc.cores=mc.cores) %>% unlist()
  } else{
    out <- lapply(starts, st_intersects_i, sf_1, sf_2, chunk_size) %>% unlist()
  }
  
  return(out)
}

# st_distance ----------------------------------------------------------------------
st_distance_chunks <- function(sdf1,sdf2,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf1),by=chunk_size)
  
  st_distance_i <- function(start, sdf1, sdf2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf1))
    distances_i <- st_distance(sdf1[start:end,],sdf2) %>% as.numeric()
    print(paste0(start, " / ", nrow(sdf1)))
    return(distances_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    distances <- pbmclapply(starts, st_distance_i, sdf1, sdf2, chunk_size, mc.cores=mc.cores) %>% unlist %>% as.numeric
  } else{
    distances <- lapply(starts, st_distance_i, sdf1, sdf2, chunk_size) %>% unlist %>% as.numeric
  }
  
  return(distances)
}

# st_distance ----------------------------------------------------------------------
st_nearest_feature_chunks <- function(sdf1,sdf2,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf1),by=chunk_size)
  
  st_nearest_feature_i <- function(start, sdf1, sdf2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf1))
    feature_ids_i <- st_nearest_feature(sdf1[start:end,],sdf2) %>% as.numeric()
    print(paste0(start, " / ", nrow(sdf1)))
    return(feature_ids_i)
  }
  
  if(mc.cores > 1){
    library(parallel)
    feature_ids <- pbmclapply(starts, st_nearest_feature_i, sdf1, sdf2, chunk_size, mc.cores=mc.cores) %>% unlist %>% as.numeric
  } else{
    feature_ids <- lapply(starts, st_nearest_feature_i, sdf1, sdf2, chunk_size) %>% unlist %>% as.numeric
  }
  
  return(feature_ids)
}

# point_to_h3 ----------------------------------------------------------------------
point_to_h3_chunks <- function(sf1, res, chunk_size){
  starts <- seq(from=1,to=nrow(sf1),by=chunk_size)
  
  point_to_h3_i <- function(start, sf1, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sf1))
    hex_ids_i <- point_to_h3(sf1[start:end,], res = res)
    print(paste0(start, " / ", nrow(sf1)))
    return(hex_ids_i)
  }
  
  hex_ids <- lapply(starts, point_to_h3_i, sf1, chunk_size) %>% unlist %>% as.character()
  
  return(hex_ids)
}

# st_join ----------------------------------------------------------------------
st_join_chunks <- function(data1_sf, data2_sf, chunk_size_1, chunk_size_2, rm_geom = F){
  starts_1 <- seq(from=1,to=nrow(data1_sf), by=chunk_size_1)
  starts_2 <- seq(from=1,to=nrow(data2_sf), by=chunk_size_2)
  
  starts_12_df <- expand_grid(
    start_1 = starts_1,
    start_2 = starts_2
  )
  
  st_join_i <- function(i, starts_12_df, chunk_size_1, chunk_size_2){
    
    print(paste0(i, " / ", nrow(starts_12_df)))
    
    start_1 <- starts_12_df$start_1[i]
    start_2 <- starts_12_df$start_2[i]
    
    end_1 <- min(start_1 + chunk_size_1 - 1, nrow(data1_sf))
    end_2 <- min(start_2 + chunk_size_2 - 1, nrow(data2_sf))
    
    out <- st_join(data1_sf[start_1:end_1,],
                   data2_sf[start_2:end_2,])
    
    if(rm_geom){
      out <- out %>% st_drop_geometry()
    }
    
    return(out)
  }
  
  df_out <- map_df(1:nrow(starts_12_df), st_join_i, starts_12_df, chunk_size_1, chunk_size_2)
  
  return(df_out)
}

st_join_chunks_parallel <- function(data1_sf,
                                    data2_sf,
                                    chunk_size_1,
                                    chunk_size_2,
                                    rm_geom = FALSE) {
  
  starts_1 <- seq(1, nrow(data1_sf), by = chunk_size_1)
  starts_2 <- seq(1, nrow(data2_sf), by = chunk_size_2)
  
  starts_12_df <- tidyr::expand_grid(
    start_1 = starts_1,
    start_2 = starts_2
  )
  
  library(furrr)
  library(progressr)
  
  opts <- furrr_options(
    globals = list(
      data1_sf = data1_sf,
      data2_sf = data2_sf,
      chunk_size_1 = chunk_size_1,
      chunk_size_2 = chunk_size_2,
      rm_geom = rm_geom
    ),
    seed = TRUE
  )
  
  handlers(global = TRUE)
  
  with_progress({
    p <- progressor(along = seq_len(nrow(starts_12_df)))
    
    future_map_dfr(
      seq_len(nrow(starts_12_df)),
      function(i) {
        
        p(sprintf("Chunk %d / %d", i, nrow(starts_12_df)))
        
        start_1 <- starts_12_df$start_1[i]
        start_2 <- starts_12_df$start_2[i]
        
        end_1 <- min(start_1 + chunk_size_1 - 1, nrow(data1_sf))
        end_2 <- min(start_2 + chunk_size_2 - 1, nrow(data2_sf))
        
        out <- sf::st_join(
          data1_sf[start_1:end_1, ],
          data2_sf[start_2:end_2, ]
        )
        
        if (rm_geom) {
          out <- sf::st_drop_geometry(out)
        }
        
        out
      },
      .options = opts
    )
  })
}
