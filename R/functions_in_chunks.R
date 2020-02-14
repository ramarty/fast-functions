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

# over ----------------------------------------------------------------------
over_chunks <- function(sdf1,sdf2,fn_type,chunk_size,mc.cores=1){
  starts <- seq(from=1,to=nrow(sdf1),by=chunk_size)
  
  over_i <- function(start, sdf1, sdf2, chunk_size){
    end <- min(start + chunk_size - 1, nrow(sdf1))
    
    if(fn_type %in% "sum") df_i <- sp::over(sdf1[start:end,], sdf2, fn=sum)
    if(fn_type %in% "mean") df_i <- sp::over(sdf1[start:end,], sdf2, fn=mean)
    if(fn_type %in% "median") df_i <- sp::over(sdf1[start:end,], sdf2, fn=median)
    if(fn_type %in% "max") df_i <- sp::over(sdf1[start:end,], sdf2, fn=max)
    
    print(start)
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