# Functions 2

require(rgeos)
library(sp)
require(dplyr)
require(parallel)

# gBuffer ----------------------------------------------------------------------
gBuffer_chunks <- function(sdf,width,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf),by=chunk_size)
  
  gBuffer_i <- function(start, sdf, width, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf))
    sdf_buff_i <- gBuffer(sdf[start:end,],width=width, byid=T)
    print(start)
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

# gBuffer ----------------------------------------------------------------------
geo.buffer_chunks <- function(sdf, r, chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf),by=chunk_size)
  
  geo.buffer_i <- function(start, sdf, width, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf))
    sdf_buff_i <- geo.buffer(sdf[start:end,],r=r)
    print(start)
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
    print(start)
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
    print(start)
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

# st_intersection ----------------------------------------------------------------------
st_intersection_chunks <- function(sf_1, sf_2, chunk_size, mc.cores=1){
  starts <- seq(from=1,to=nrow(sf_1),by=chunk_size)
  
  st_intersection_i <- function(start, sf_1, sf_2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sf_1))
    sf_1_i <- st_intersection(sf_1[start:end,],sf_2)
    print(start)
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

# st_distance ----------------------------------------------------------------------
st_distance_chunks <- function(sdf1,sdf2,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf1),by=chunk_size)
  
  st_distance_i <- function(start, sdf1, sdf2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf1))
    distances_i <- st_distance(sdf1[start:end,],sdf2) %>% as.numeric()
    print(start)
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





