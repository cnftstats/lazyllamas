# Setup --------------------------------------------------------------------------------------------
library(data.table)
library(lubridate)
library(jsonlite)
library(tidyr)
library(httr)

# Variables ----------------------------------------------------------------------------------------
policy_id <- "9e7b9873fc65bc20ada9739b85d15057603577c1777c7325bba9ae9c"
project <- "Lazy llamas"
project_label <- "lazyllamas"
time_now <- as_datetime(now())


# Databases ----------------------------------------------------------------------------------------
RAR <- readRDS(sprintf("data/RAR_%s.rds", project_label))


# Functions ----------------------------------------------------------------------------------------
extract_num <- function(x) as.numeric(regmatches(x, regexpr("[[:digit:]]+", x)))

loj <- function (X = NULL, Y = NULL, onCol = NULL) {
  if (truelength(X) == 0 | truelength(Y) == 0) 
    stop("setDT(X) and setDT(Y) first")
  n <- names(Y)
  X[Y, `:=`((n), mget(paste0("i.", n))), on = onCol]
}


# JPG listings -------------------------------------------------------------------------------------
JPG_list <- list()
p <- 1
while (TRUE) {
  api_link <- sprintf("https://server.jpgstoreapis.com/policy/%s/listings?page=%d", policy_id, p)
  X <- data.table(fromJSON(rawToChar(GET(api_link)$content)))
  if (nrow(X) == 0) break
  JPG_list[[p]] <- X
  p <- p + 1
}

JPG <- rbindlist(JPG_list)

JPG[, link           := paste0("https://www.jpg.store/asset/", asset_id)]
JPG[, asset          := display_name]
JPG[, price          := price_lovelace/10**6]
JPG[, sc             := "yes"]
JPG[, market         := "jpg.store"]
JPG[, asset_number   := extract_num(asset)]
JPG[is.na(asset_number), asset_number := strsplit(asset, "")]

JPG <- JPG[, .(asset, asset_number, type = "listing", price, last_offer = NA, sc, market, link)]


# JPG sales ----------------------------------------------------------------------------------------
JPGS_list <- lapply(1:7, function(p) {
  api_link <- sprintf("https://server.jpgstoreapis.com/policy/%s/sales?page=%d", policy_id, p)
  X <- data.table(fromJSON(rawToChar(GET(api_link)$content)))
  return(X)
})

JPGS <- rbindlist(JPGS_list)

JPGS[, asset          := display_name]
JPGS[, price          := price_lovelace/10**6]
JPGS[, market         := "jpg.store"]
JPGS[, asset_number   := extract_num(asset)]
JPGS[, sold_at        := as_datetime(confirmed_at)]
JPGS[, sold_at_hours  := difftime(time_now, sold_at, units = "hours")]
JPGS[, sold_at_days   := difftime(time_now, sold_at, units = "days")]

JPGS <- JPGS[order(-sold_at), .(asset, asset_number, price, sold_at, sold_at_hours,
                                sold_at_days, market)]
JPGS <- JPGS[sold_at_hours <= 24*3]


# Merge markets data -------------------------------------------------------------------------------
# Listings
DT <- copy(JPG)

# Sales
DTS <- copy(JPGS)

# Add data collection timestamp
DT[, data_date := time_now]
DTS[, data_date := time_now]


# Rarity and ranking -------------------------------------------------------------------------------
setDT(DT); setDT(RAR)
loj(DT, RAR, "asset_number")

setDT(DTS); setDT(RAR)
loj(DTS, RAR, "asset_number")


# Large format -------------------------------------------------------------------------------------
.cols <- names(DT)[names(DT) %like% "asset_trait_"]
DTL <- data.table(gather(DT, key, value, all_of(.cols)))

DTL[, trait_category := strsplit(value, "_")[[1]][1], 1:nrow(DTL)]
DTL[, trait_category := gsub("\\.", " ", trait_category)]
DTL[, trait          := strsplit(value, "_")[[1]][2], 1:nrow(DTL)]


# Save ---------------------------------------------------------------------------------------------
saveRDS(DT, file = "data/DT.rds")
saveRDS(DTL, file = "data/DTL.rds")
saveRDS(DTS, file = "data/DTS.rds")


# Database evolution -------------------------------------------------------------------------------
DTE <- copy(DT)
.file_name <- "data/DTE.rds"
if (file.exists(.file_name)) {
  cat("File data/DTE exists:", file.exists(.file_name), "\n")
  DTE_old <- readRDS(.file_name)
  DTE <- rbindlist(list(DTE, DTE_old), fill = TRUE)
  DTE <- DTE[difftime(time_now, data_date, units = "hours") <= 24] # Only retain last 24 hours
}
saveRDS(DTE, file = .file_name)
