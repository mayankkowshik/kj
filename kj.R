library(readr)
library(dplyr)
library(data.table)

df <- read_csv("C:/Users/mayank.kowshik/Desktop/Kalyan/Trivandrum.csv")
df <- data.frame(df$BillDate , df$CustomerCode)
colnames(df) <- c("date","customercode")
df <- df[!duplicated(df[1:2]),]

df <- data.table(df)
df <- df[, counter := .N, by = customercode]
df <- df[(df$counter > 5),  ]
df <- df[order(df$date),]
df <- df[order(df$customercode),]
df <- data.frame(df)
df$date <- as.Date(df$date)

customer_list <- unique(df$customercode)
customer_list <- data.frame(customer_list)
date_list <- unique(df$date)
date_list <- data.frame(date_list)
print(summary(df))
write_csv(df,"C:/Users/mayank.kowshik/Desktop/Kalyan/trivundrumcleaned.csv")

#read clean data 
df <- read_csv("C:/Users/mayank.kowshik/Desktop/Kalyan/trivundrumcleaned.csv")
df$numberofdays <- 0

#Setting up parallel execution
library(foreach)
library(doParallel)
cores=detectCores()
cl <- makeCluster(cores[1]-1) #not to overload your computer
registerDoParallel(cl)
k <- nrow(df)-1
start.time <- Sys.time()
numberofdays <- foreach(i = 1:k , .combine='rbind') %dopar%
{
  if( df$customercode[i] == df$customercode[i+1] )
     df$numberofdays[i] <- (df$date[i+1] - df$date[i]) 
  
    else
     df$numberofdays[i] <- 0
}

end.time <- Sys.time()
print(end.time - start.time)
stopCluster(cl)




#Appending columns to data frame
df <- df[1:length(numberofdays), ]
numberofdays <- data.frame(numberofdays)
df <- cbind(df,numberofdays)
#rm(list=ls(all=TRUE))

#Poisson Model
library(foreach)
library(doParallel)
cores=detectCores()
cl <- makeCluster(cores[1]-1) #not to overload your computer
registerDoParallel(cl)
k <- nrow(df)-1
df$poisson <- 0
start.time <- Sys.time()
poisson <- foreach (i = 1:k , .combine='rbind')  %dopar%
{
  if(df$customercode[i] == df$customercode[i+1])
    df$poisson[i+1] <- (df$numberofdays[i+1] + df$numberofdays[i]) 
  
  else
    df$poisson[i] <- df$numberofdays[i]
}

end.time <- Sys.time()
print(end.time - start.time)
stopCluster(cl)


