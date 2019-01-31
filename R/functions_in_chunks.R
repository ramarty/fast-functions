# Functions 2

library(dplyr)

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
    sdf_buff <- mclapply(starts, gBuffer_i, sdf, width, chunk_size, mc.cores=mc.cores) %>% do.call(what="rbind")
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
    distances <- mclapply(starts, gDistance_i, sdf1, sdf2, chunk_size, mc.cores=mc.cores) %>% unlist %>% as.numeric
  } else{
    distances <- lapply(starts, gDistance_i, sdf1, sdf2, chunk_size) %>% unlist %>% as.numeric
  }
  
  return(distances)
}