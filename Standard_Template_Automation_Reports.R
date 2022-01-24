options(java.parameters = "-Xmx8000m")
#Libraries

library(tidyr)
library(dplyr)
library("xlsx")
library(googlesheets)
library(RCurl)
library(httpuv)
library(stringr)
library(mailR)
library(RPostgreSQL)
library(RJDBC)

#Reading Googlesheet to take custom input of advertiser
#gs_auth(token = "/data/R/tokens/filename.rds")
gs_auth(token = "filename.rds")

myCsv <- gs_url("https://docs.google.com/spreadsheets/d/sheetkey/edit#gid=0")

data_main <- gs_read(myCsv)
rownum <- which(grepl("Yes", data_main$Insight_Flag)) +1
position <- paste("H",rownum,sep = "")
str(data_main)
gs_edit_cells(myCsv, ws = "Sheet1", anchor = position[1], input = c("No"), byrow = FALSE)
#After reading googlesheet. "Yes" Flag is changed to "No", so next run will start from the new "Yes" Flag

#Setting connection to Amazon S3 Data Warehouse to load advertisement data
newconn <- RPostgreSQL::dbConnect("PostgreSQL",
                                  host = "hostname.us-east-1.redshift.amazonaws.com",
                                  user = "user", password = "*******",port =5439,dbname="warehouse")


# Converting input from Google sheet into Dataframe of variables
data_main$Advertiser_ID[rownum[1]-1] <- str_replace_all(data_main$Advertiser_ID[rownum[1]-1], pattern = "\\|", replacement = "\\,")
data_main$Insertion_Order_ID[rownum[1]-1] <- str_replace_all(data_main$Insertion_Order_ID[rownum[1]-1], pattern = "\\|", replacement = "\\,")
data_main$Conversion_Pixel_ID[rownum[1]-1] <- str_replace_all(data_main$Conversion_Pixel_ID[rownum[1]-1], pattern = "\\|", replacement = "\\,")
data_main$Segment_Pixel_ID[rownum[1]-1] <- str_replace_all(data_main$Segment_Pixel_ID[rownum[1]-1], pattern = "\\|", replacement = "\\,")



#Reading Insertion Order Id
readinsertion_order_id <- function()
{ insertion_order_id <- paste0(unlist(str_split(data_main$Insertion_Order_ID[rownum[1]-1],",")),sep="",collapse="','")
if ((insertion_order_id)=="NA"){
  return()
}
return(insertion_order_id)
}

#Reading Conversion Pixel_Id
readconversionpixel_id <- function()
{ 
  pixel_id<-paste0(unlist(str_split(data_main$Conversion_Pixel_ID[rownum[1]-1],",")),sep="",collapse="','")
  if ((pixel_id)=="NA"){
    return()
  }
  return(pixel_id)
}  

#Reading Segment Pixel_Id
readsegmentpixel_id <- function()
{ 
  pixel_id<-paste0(unlist(str_split(data_main$Segment_Pixel_ID[rownum[1]-1],",")),sep="",collapse="','")
  if ((pixel_id)=="NA"){
    return()
  }
  return(pixel_id)
}  

#Reading from_dt, to_dt
readfrom_dt <- function()
{ 
  from_dt <- paste0(as.character(data_main$`From_Date(YYYY-MM-DD)`[rownum[1]-1]),sep="",collapse = "','")
  from_dt <- as.character.Date(from_dt)
  return(from_dt)
}
readto_dt <- function()
{ 
  to_dt <- paste0(as.character(data_main$`To_Date(YYYY-MM-DD)`[rownum[1]-1]),sep="",collapse = "','")
  to_dt <- as.character.Date(to_dt)
  return(to_dt)
}

#Reading advertiser_id
readadvertiser_id <- function()
{ 
  advertiser_id <- paste0(unlist(str_split(data_main$Advertiser_ID[rownum[1]-1],",")),sep="",collapse="','")
  if ((advertiser_id)=="NA"){
    return()
  }
  return(advertiser_id)
}


#Reading Time_Zone
readtimezone <- function()
{ 
  timezone <- paste0(as.character(data_main$Timezone[rownum[1]-1]),sep="",collapse="','")
  if ((timezone)=="NA"){
    return()
  }
  return(timezone)
}

#Saving in variables
advertiser_id<-c(readadvertiser_id())
#tryCatch(expr=is.null(advertiser_id),finally = quit())
insertion_order_id<- readinsertion_order_id()
from_dt<-(readfrom_dt())
to_dt<-(readto_dt())
time_zone<-(readtimezone())
conv_pixel_id<-(readconversionpixel_id())
seg_pixel_id<-(readsegmentpixel_id())

#File Name creation
filename=paste0(gsub(x = advertiser_id, pattern = "'", replacement = ""),"_")

#Start of TOD query

headerstr<-"select day_hour,extract(dw from dt) as dw, sum(impressions) as impressions, sum(clicks) as clicks, sum(pcconversions+pvconversions) as conversions from report_placement_hour where time_zone_name in ('"
time_zone<-paste0(time_zone,"')")
adstr<-paste0("and advertiser_id in ('",paste0(advertiser_id),"')")

iostr<-if(is.null(insertion_order_id)){""}else{paste0("and insertion_order_id in ('",paste0(insertion_order_id),"')")}

dayrange<-paste0("and dt >='",from_dt,"'and dt <='",to_dt,"'")
pix<-if(is.null(conv_pixel_id)){""}else{paste0(" and pixel_id in ('",paste0(conv_pixel_id,"','0')"))}

query<-paste0(headerstr,time_zone,adstr,iostr,dayrange,pix," group by 1,2;")

raw_data <- dbGetQuery(newconn,query)


#coping raw data
data_cleaning<-raw_data

#removing Na
data_clean <- data_cleaning[complete.cases(data_cleaning),]
y<- data.frame(dw=as.integer(c("0","1","2","3","4","5","6")),day_name=c("Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"),stringsAsFactors=FALSE)

data_grouped <- data_clean %>% 
  select(day_hour, impressions, clicks,conversions,dw) %>%
  left_join(y, by="dw")%>%
  filter(!is.na(day_name))
data_grouped1<- data_grouped%>%
  select(day_hour, day_name, impressions, clicks,conversions)%>%
  filter(day_hour>6)%>%
  arrange(day_hour,day_name)

x<-data_frame(day_name=c("Monday","Tuesday","Wednesday","Thursday","Friday"),stringsAsFactors=FALSE)
data_grouped_Weekday<-data_grouped1%>%
  select(day_hour, impressions, clicks,conversions, day_name)%>%
  left_join(x, by="day_name")%>%
  filter(!is.na(stringsAsFactors))%>%
  select(day_hour, impressions, clicks,conversions)%>%
  group_by(day_hour)%>%
  summarise(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions)) %>%
  mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
  select(day_hour,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)

write.xlsx2(data_grouped_Weekday, file=paste0(filename,"Report.xlsx"), sheetName="TOD_Weekday",
            col.names=TRUE, row.names=TRUE, append=FALSE)

x<-data_frame(day_name=c("Saturday","Sunday"),stringsAsFactors=FALSE)
data_grouped_Weekend<-data_grouped1%>%
  select(day_hour, day_name, impressions, clicks,conversions)%>%
  left_join(x, by="day_name")%>%
  filter(!is.na(stringsAsFactors))%>%
  select(day_hour, impressions, clicks,conversions)%>%
  group_by(day_hour)%>%
  summarise(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions)) %>%
  mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
  select(day_hour,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)


write.xlsx2(data_grouped_Weekend, file=paste0(filename,"Report.xlsx"), sheetName="TOD_Weekend",
            col.names=TRUE, row.names=TRUE, append=TRUE)

#End of TOD

#Start of DOW


x<-c("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")
data_grouped <- data_grouped %>% 
  select(day_name, impressions, clicks,conversions) %>% 
  group_by(day_name) %>%
  summarise(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions)) %>%
  arrange(day_name)%>%
  mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
  select(day_name,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)%>%
  mutate(day_name =  factor(day_name, levels = x)) %>%
  arrange(day_name)

write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="DOW",
            col.names=TRUE, row.names=TRUE, append=TRUE)

#End of DOW

#Start of Site_Domains

headerstr<-"select site_domain, sum(impressions) as impressions, sum(clicks) as clicks, sum(pcconversions+pvconversions) as conversions from report_inventory_appnexus where "
adstr<-paste0("advertiser_id in ('",paste0(advertiser_id),"')")

iostr<-if(is.null(insertion_order_id)){""}else{paste0("and insertion_order_id in ('",paste0(insertion_order_id),"')")}

dayrange<-paste("and dt >='",from_dt,"'and dt <='",to_dt,"'")
pix<-if(is.null(conv_pixel_id)){""}else{paste0(" and pixel_id in ('",paste0(conv_pixel_id,"','0')"))}

query<-paste0(headerstr,adstr,iostr,dayrange,pix," GROUP BY 1;")

raw_data <- dbGetQuery(newconn,query)

#coping raw data
data_cleaning<-raw_data

#removing Na
data_clean <- data_cleaning[complete.cases(data_cleaning),]
data_grouped <- data_clean %>% 
  select(site_domain, impressions, clicks, conversions) %>% 
  group_by(site_domain) %>%
  summarise(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions)) %>%
  arrange(site_domain)%>%
  mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
  filter(Percentage_Impression_share>0.01)%>%
  arrange(desc(Percentage_Impression_share))%>%
  select(site_domain,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)


write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="Site_Domains",
            col.names=TRUE, row.names=TRUE, append=TRUE)
#End of Site_Domains

#Start of Device_type
headerstr<-"select distinct(device_type), sum(impressions) as impressions, sum(clicks) as clicks, sum(pcconversions+pvconversions) as conversions from report_system_appnexus where "
adstr<-paste0("advertiser_id in ('",paste0(advertiser_id),"')")

iostr<-if(is.null(insertion_order_id)){""}else{paste0("and insertion_order_id in ('",paste0(insertion_order_id),"')")}

dayrange<-paste0("and dt >='",from_dt,"'and dt <='",to_dt,"'")
pix<-if(is.null(conv_pixel_id)){""}else{paste0(" and pixel_id in ('",paste0(conv_pixel_id,"','0')"))}

query<-paste0(headerstr,adstr,iostr,dayrange,pix," GROUP BY 1;")
raw_data <- dbGetQuery(newconn,query)

#coping raw data
data_cleaning<-raw_data
#removing Na
data_clean <- data_cleaning[complete.cases(data_cleaning),]

x<-data.frame(device_type=c("pc & other devices","pc or unknown","pc","mobile","phone","Phone","tablet","Tablet"),device_types=c("pc","pc","pc","phone","phone","phone","tablet","tablet"),stringsAsFactors=FALSE)

data_grouped <- data_clean %>% 
  select(device_type,impressions, clicks, conversions) %>% 
  select(impressions, clicks, conversions,device_type) %>% 
  left_join(x,by="device_type")%>%
  select(device_types,impressions, clicks, conversions) %>%
  group_by(device_types)%>%
  filter(!is.na(device_types))%>%
  summarise(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions)) %>%
  select(device_types, impressions, clicks, conversions) %>% 
  arrange(device_types)%>%
  mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
  select(device_types,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)


write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="Device_type",
            col.names=TRUE, row.names=TRUE, append=TRUE)

#End of Device

#Start of Device OS

headerstr<-"select device_type,os_name, sum(impressions) as impressions, sum(clicks) as clicks, sum(pcconversions+pvconversions) as conversions from report_system_appnexus where "
adstr<-paste0("advertiser_id in ('",paste0(advertiser_id),"')")

iostr<-if(is.null(insertion_order_id)){""}else{paste0("and insertion_order_id in ('",paste0(insertion_order_id),"')")}

dayrange<-paste0("and dt >='",from_dt,"'and dt <='",to_dt,"'")
pix<-if(is.null(conv_pixel_id)){""}else{paste0(" and pixel_id in ('",paste0(conv_pixel_id,"','0')"))}

query<-paste0(headerstr,adstr,iostr,dayrange,pix," GROUP BY 1,2;")
raw_data <- dbGetQuery(newconn,query)

#coping raw data
data_cleaning<-raw_data
#removing Na
data_clean <- data_cleaning[complete.cases(data_cleaning),]

x<-data.frame(device_type=c("pc & other devices","pc or unknown","pc","mobile","phone","Phone","tablet","Tablet"),device_types=c("pc","pc","pc","phone","phone","phone","tablet","tablet"),stringsAsFactors=FALSE)

data_grouped <- data_clean %>% 
  select(device_type,os_name,impressions, clicks, conversions) %>% 
  select(os_name, impressions, clicks, conversions,device_type) %>% 
  left_join(x,by="device_type")%>%
  select(device_types, os_name,impressions, clicks, conversions) %>%
  group_by(device_types, os_name)%>%
  filter(!is.na(device_types))%>%
  summarise(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions)) %>%
  select(device_types, os_name,impressions, clicks, conversions) %>% 
  arrange(device_types,os_name)%>%
  mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
  select(device_types, os_name,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)%>%
  ungroup()

write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="Device_Os",
            col.names=TRUE, row.names=TRUE, append=TRUE)

#End of Device OS

#Start of Device Browser

headerstr<-"SELECT device_type, browser_name, SUM(impressions) AS impressions,SUM(clicks) AS clicks, SUM(pcconversions + pvconversions) AS conversions FROM report_system_appnexus WHERE "
adstr<-paste0("advertiser_id in ('",paste0(advertiser_id),"')")

iostr<-if(is.null(insertion_order_id)){""}else{paste0("and insertion_order_id in ('",paste0(insertion_order_id),"')")}

dayrange<-paste0("and dt >='",from_dt,"'and dt <='",to_dt,"'")
pix<-if(is.null(conv_pixel_id)){""}else{paste0(" and pixel_id in ('",paste0(conv_pixel_id,"','0')"))}

query<-paste0(headerstr,adstr,iostr,dayrange,pix," GROUP BY 1,2;")
raw_data <- dbGetQuery(newconn,query)

#coping raw data
data_cleaning<-raw_data
#removing Na
data_clean <- data_cleaning[complete.cases(data_cleaning),]
x<-data.frame(device_type=c("pc & other devices","pc or unknown","pc","mobile","phone","Phone","tablet","Tablet"),device_types=c("pc","pc","pc","phone","phone","phone","tablet","tablet"),stringsAsFactors=FALSE)

data_grouped <- data_clean %>% 
  select(device_type,browser_name,impressions, clicks, conversions) %>% 
  select(browser_name, impressions, clicks, conversions,device_type) %>% 
  left_join(x,by="device_type")%>%
  select(device_types, browser_name,impressions, clicks, conversions) %>%
  group_by(device_types, browser_name)%>%
  filter(!is.na(device_types))%>%
  summarise(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions)) %>%
  select(device_types, browser_name,impressions, clicks, conversions) %>% 
  arrange(device_types, browser_name)%>%
  mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
  select(device_types, browser_name,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR) %>% 
  ungroup()

write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="Browser",
            col.names=TRUE, row.names=TRUE, append=TRUE)

#End of Device Browser

#Start of Creative

headerstr<-"SELECT width, height,SUM(impressions) AS impressions,SUM(clicks) AS clicks, SUM(pcconversions + pvconversions) AS conversions from report_system_appnexus WHERE "
adstr<-paste0("advertiser_id in ('",paste0(advertiser_id),"')")

iostr<-if(is.null(insertion_order_id)){""}else{paste0("and insertion_order_id in ('",paste0(insertion_order_id),"')")}

dayrange<-paste0("and dt >='",from_dt,"'and dt <='",to_dt,"'")
pix<-if(is.null(conv_pixel_id)){""}else{paste0(" and pixel_id in ('",paste0(conv_pixel_id,"','0')"))}

query<-paste0(headerstr,adstr,iostr,dayrange,pix," GROUP BY 1,2;")
raw_data <- dbGetQuery(newconn,query)

#coping raw data
data_cleaning<-raw_data
#removing Na
data_clean <- data_cleaning[complete.cases(data_cleaning),]


data_grouped <- data_clean %>% 
  select(width, height, impressions, clicks, conversions)%>%
  group_by(width,height)%>%
  filter(!is.na(width))%>%
  filter(!is.na(height))%>%
  ungroup()%>%
  select(width, height,impressions, clicks, conversions) %>% 
  mutate(size=paste0(width,"x",height))%>%
  select(size, impressions,clicks, conversions)%>%
  mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
  select(size,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)

write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="Creative_Size",
            col.names=TRUE, row.names=TRUE, append=TRUE)

#End of Creative

#Start of CityRegions
headerstr<-paste0("select de_city as city, de_region_name as region, sum(impressions) as impressions, sum(clicks) as clicks, sum(pvconversions + pcconversions) as conversions from report_geo_de_appnexus where de_country_name in ",if(time_zone=="AEST')"){"('australia','AU','Australia')"}else{""},if(time_zone=="NZST')"){"('new zealand','NZ','New Zealand')"}else{""}," and ")
adstr<-paste0("advertiser_id in ('",paste0(advertiser_id),"')")

iostr<-if(is.null(insertion_order_id)){""}else{paste0("and insertion_order_id in ('",paste0(insertion_order_id),"')")}

dayrange<-paste0("and dt >='",from_dt,"'and dt <='",to_dt,"'")
pix<-if(is.null(conv_pixel_id)){""}else{paste0(" and pixel_id in ('",paste0(conv_pixel_id,"','0')"))}

query<-paste0(headerstr,adstr,iostr,dayrange,pix, " group by 1,2;")
raw_data <- dbGetQuery(newconn,query)

#coping raw data
data_cleaning<-raw_data
#removing Na
data_clean <- data_cleaning[complete.cases(data_cleaning),]
if(time_zone=="AEST')"){
  x<- data.frame(region=c('australian capital territory',
                          'new south wales', 
                          'northern territory',
                          'queensland',
                          'south australia', 
                          'tasmania', 
                          'victoria',
                          'western australia',
                          'Australian Capital Territory',
                          'New South Wales', 
                          'Northern Territory',
                          'Queensland',
                          'South Australia', 
                          'Tasmania',
                          'Victoria',
                          'Western Australia'),regions=c("ACT","NSW","NT","QLD","SA","TAS","VIC","WA") ,stringsAsFactors=FALSE)
  y<- data.frame(regions=c("ACT","NSW","NT","QLD","SA","TAS","VIC","WA"),region_code=c("01","02","03","04","05","06","07","08"),stringsAsFactors=FALSE)
  
  data_clean$city<-sapply(data_clean$city, tolower)
  data_grouped <- data_clean %>% 
    select(city,impressions,clicks,conversions,region)%>%
    left_join(x,by = "region")%>%
    left_join(y,by = "regions")%>%
    select(city,impressions,clicks,conversions,regions,region_code)%>%
    group_by(city,regions,region_code)%>%
    summarize(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions))%>%ungroup()%>%
    mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
    select(city,regions,region_code,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)%>%
    filter(!is.na(city))%>%
    filter(!is.na(regions))%>%
    arrange(desc(Percentage_Impression_share))
  
  write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="City_Region",
              col.names=TRUE, row.names=TRUE, append=TRUE)
}
if(time_zone=="NZST')"){
  data_clean$city<-sapply(data_clean$city, tolower)
  data_clean$region<-sapply(data_clean$region, tolower)
  x<- data.frame(region=c("northland", 
                          "auckland",
                          "waikato",
                          "bay of plenty",
                          "gisborne",
                          "hawkes bay",
                          "hawke's bay",
                          "taranaki",
                          "manawatu-wanganui",
                          "wellington",
                          "tasman",
                          "nelson",
                          "marlborough",
                          "west coast",
                          "canterbury",
                          "otago",
                          "southland"),regions=c(
                            "Northland",
                            "Auckland",
                            "Waikato",
                            "Bay Of Plenty",
                            "Gisborne",
                            "Hawke's Bay",
                            "Hawke's Bay",
                            "Taranaki",
                            "Manawatu-Wanganui",
                            "Wellington",
                            "Tasman",
                            "Nelson",
                            "Marlborough",
                            "West Coast",
                            "Canterbury",
                            "Otago",
                            "Southland"
                          ) ,stringsAsFactors=FALSE)
  y<- data.frame(regions=c(
    "Northland",
    "Auckland",
    "Waikato",
    "Bay Of Plenty",
    "Gisborne",
    "Hawke's Bay",
    "Taranaki",
    "Manawatu-Wanganui",
    "Wellington",
    "Tasman",
    "Nelson",
    "Marlborough",
    "West Coast",
    "Canterbury",
    "Otago",
    "Southland"
  ),region_code=c("01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16"),stringsAsFactors=FALSE)
  
  data_grouped <- data_clean %>% 
    select(city,impressions,clicks,conversions,region)%>%
    left_join(x,by = "region")%>%
    left_join(y,by = "regions")%>%
    select(city,impressions,clicks,conversions,regions,region_code)%>%
    group_by(city,regions,region_code)%>%
    summarize(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions))%>%ungroup()%>%
    mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
    select(city,regions,region_code,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)%>%
    filter(!is.na(city))%>%
    filter(!is.na(regions))%>%
    arrange(desc(Percentage_Impression_share))
  
  write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="City_Region",
              col.names=TRUE, row.names=TRUE, append=TRUE)
  
}
#End of CityRegions

print("End of cityregions")
#Start of RegionCode

if(time_zone=="AEST')"){
  x<- data.frame(region=c('australian capital territory',
                          'new south wales', 
                          'northern territory',
                          'queensland',
                          'south australia', 
                          'tasmania', 
                          'victoria',
                          'western australia',
                          'Australian Capital Territory',
                          'New South Wales', 
                          'Northern Territory',
                          'Queensland',
                          'South Australia', 
                          'Tasmania',
                          'Victoria',
                          'Western Australia'),regions=c("ACT","NSW","NT","QLD","SA","TAS","VIC","WA") ,stringsAsFactors=FALSE)
  y<- data.frame(regions=c("ACT","NSW","NT","QLD","SA","TAS","VIC","WA"),region_code=c("01","02","03","04","05","06","07","08"),stringsAsFactors=FALSE)
  data_clean$city<-sapply(data_clean$city, tolower)
  
  data_grouped <- data_clean %>% 
    select(impressions,clicks,conversions,region)%>%
    left_join(x,by = "region")%>%
    left_join(y,by = "regions")%>%
    select(impressions,clicks,conversions,regions,region_code)%>%
    group_by(regions,region_code)%>%
    filter(!is.na(regions))%>%
    summarize(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions))%>%ungroup()%>%
    mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
    select(regions,region_code,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)%>%
    arrange(region_code)
  
  write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="Regions",
              col.names=TRUE, row.names=TRUE, append=TRUE)
  
}

if(time_zone=="NZST')"){
  data_clean$city<-sapply(data_clean$city, tolower)
  data_clean$region<-sapply(data_clean$region, tolower)
  x<- data.frame(region=c("northland", 
                          "auckland",
                          "waikato",
                          "bay of plenty",
                          "gisborne",
                          "hawkes bay",
                          "hawke's bay",
                          "taranaki",
                          "manawatu-wanganui",
                          "wellington",
                          "tasman",
                          "nelson",
                          "marlborough",
                          "west coast",
                          "canterbury",
                          "otago",
                          "southland"),regions=c(
                            "Northland",
                            "Auckland",
                            "Waikato",
                            "Bay Of Plenty",
                            "Gisborne",
                            "Hawke's Bay",
                            "Hawke's Bay",
                            "Taranaki",
                            "Manawatu-Wanganui",
                            "Wellington",
                            "Tasman",
                            "Nelson",
                            "Marlborough",
                            "West Coast",
                            "Canterbury",
                            "Otago",
                            "Southland"
                          ) ,stringsAsFactors=FALSE)
  y<- data.frame(regions=c(
    "Northland",
    "Auckland",
    "Waikato",
    "Bay Of Plenty",
    "Gisborne",
    "Hawke's Bay",
    "Taranaki",
    "Manawatu-Wanganui",
    "Wellington",
    "Tasman",
    "Nelson",
    "Marlborough",
    "West Coast",
    "Canterbury",
    "Otago",
    "Southland"
  ),region_code=c("01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16"),stringsAsFactors=FALSE)
  
  data_grouped <- data_clean %>% 
    select(impressions,clicks,pcconversions,pvconversions,region)%>%
    left_join(x,by = "region")%>%
    left_join(y,by = "regions")%>%
    select(impressions,clicks,pcconversions,pvconversions,regions,region_code)%>%
    group_by(regions,region_code)%>%
    filter(!is.na(regions))%>% summarize(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(pcconversions+pvconversions))%>%ungroup()%>%
    mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
    select(regions,region_code,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)%>%
    arrange(region_code)
  
  write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="Regions",
              col.names=TRUE, row.names=TRUE, append=TRUE)
  
}

#End of RegionCode

#Start of CityPostcodes
headerstr<-paste0("select de_city as city, de_region_name as region, postal_code, sum(impressions) as impressions, sum(clicks) as clicks, sum(pvconversions + pcconversions) as conversions from report_geo_de_appnexus where de_country_name in ",if(time_zone=="AEST')"){"('australia','AU','Australia')"}else{""},if(time_zone=="NZST')"){"('new zealand','NZ','New Zealand')"}else{""}," and ")
adstr<-paste0("advertiser_id in ('",paste0(advertiser_id),"')")

iostr<-if(is.null(insertion_order_id)){""}else{paste0("and insertion_order_id in ('",paste0(insertion_order_id),"')")}

dayrange<-paste0("and dt >='",from_dt,"'and dt <='",to_dt,"'")
pix<-if(is.null(conv_pixel_id)){""}else{paste0(" and pixel_id in ('",paste0(conv_pixel_id,"','0')"))}

query<-paste0(headerstr,adstr,iostr,dayrange,pix, " group by 1,2,3;")
raw_data <- dbGetQuery(newconn,query)

#coping raw data
data_cleaning<-raw_data
#removing Na
data_clean <- data_cleaning[complete.cases(data_cleaning),]
if(time_zone=="AEST')"){
  x<- data.frame(region=c('australian capital territory',
                          'new south wales', 
                          'northern territory',
                          'queensland',
                          'south australia', 
                          'tasmania', 
                          'victoria',
                          'western australia',
                          'Australian Capital Territory',
                          'New South Wales', 
                          'Northern Territory',
                          'Queensland',
                          'South Australia', 
                          'Tasmania',
                          'Victoria',
                          'Western Australia'),regions=c("ACT","NSW","NT","QLD","SA","TAS","VIC","WA") ,stringsAsFactors=FALSE)
  y<- data.frame(regions=c("ACT","NSW","NT","QLD","SA","TAS","VIC","WA"),region_code=c("01","02","03","04","05","06","07","08"),stringsAsFactors=FALSE)
  
  data_clean$city<-sapply(data_clean$city, tolower)
  data_grouped <- data_clean %>% 
    select(city, postal_code,impressions,clicks,conversions,region)%>%
    left_join(x,by = "region")%>%
    left_join(y,by = "regions")%>%
    select(city, postal_code,impressions,clicks,conversions,regions,region_code)%>%
    group_by(city,regions,region_code, postal_code)%>%
    summarize(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions))%>%ungroup()%>%
    mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
    select(city,regions,region_code, postal_code,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)%>%
    filter(!is.na(city))%>%
    filter(!is.na(regions))%>%
    arrange(desc(Percentage_Impression_share))
  
  write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="City_postal_code",
              col.names=TRUE, row.names=TRUE, append=TRUE)
}
if(time_zone=="NZST')"){
  data_clean$city<-sapply(data_clean$city, tolower)
  data_clean$region<-sapply(data_clean$region, tolower)
  x<- data.frame(region=c("northland", 
                          "auckland",
                          "waikato",
                          "bay of plenty",
                          "gisborne",
                          "hawkes bay",
                          "hawke's bay",
                          "taranaki",
                          "manawatu-wanganui",
                          "wellington",
                          "tasman",
                          "nelson",
                          "marlborough",
                          "west coast",
                          "canterbury",
                          "otago",
                          "southland"),regions=c(
                            "Northland",
                            "Auckland",
                            "Waikato",
                            "Bay Of Plenty",
                            "Gisborne",
                            "Hawke's Bay",
                            "Hawke's Bay",
                            "Taranaki",
                            "Manawatu-Wanganui",
                            "Wellington",
                            "Tasman",
                            "Nelson",
                            "Marlborough",
                            "West Coast",
                            "Canterbury",
                            "Otago",
                            "Southland"
                          ) ,stringsAsFactors=FALSE)
  y<- data.frame(regions=c(
    "Northland",
    "Auckland",
    "Waikato",
    "Bay Of Plenty",
    "Gisborne",
    "Hawke's Bay",
    "Taranaki",
    "Manawatu-Wanganui",
    "Wellington",
    "Tasman",
    "Nelson",
    "Marlborough",
    "West Coast",
    "Canterbury",
    "Otago",
    "Southland"
  ),region_code=c("01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16"),stringsAsFactors=FALSE)
  
  data_grouped <- data_clean %>% 
    select(city, postal_code,impressions,clicks,conversions,region)%>%
    left_join(x,by = "region")%>%
    left_join(y,by = "regions")%>%
    select(city, postal_code,impressions,clicks,conversions,regions,region_code)%>%
    group_by(city,regions,region_code, postal_code)%>%
    summarize(impressions=sum(impressions),clicks=sum(clicks),conversions=sum(conversions))%>%ungroup()%>%
    mutate( Percentage_CTR = (ifelse(impressions==0,0,clicks*100/impressions)),Percentage_CVR = (ifelse(impressions==0,0,conversions*100/impressions)),Percentage_Click_share = (if(sum(clicks)==0){0}else{clicks*100/sum(clicks)}),Percentage_Conversion_share = (if(sum(conversions)==0){0}else{conversions*100/sum(conversions)}),Percentage_Impression_share = (if(sum(impressions)==0){0}else{impressions*100/sum(impressions)}))%>%
    select(city,regions,region_code, postal_code,impressions,Percentage_Impression_share,clicks,Percentage_Click_share,Percentage_CTR,conversions,Percentage_Conversion_share,Percentage_CVR)%>%
    filter(!is.na(city))%>%
    filter(!is.na(regions))%>%
    arrange(desc(Percentage_Impression_share))
  
  write.xlsx2(data_grouped, file=paste0(filename,"Report.xlsx"), sheetName="City_postal_code",
              col.names=TRUE, row.names=TRUE, append=TRUE)
  
}
#End of CityPostcodes



filename=paste0(filename,"Report.xlsx")

#sending Email

to<- paste(data_main$To_email_address[rownum[1]-1])
email <- send.mail(from = "****",
                   to=to,
                   subject = "Automation of Standard Insights",
                   body = paste0("Hey,
                                 Attached is the data for your advertiser: ",gsub(x = advertiser_id, pattern = "'", replacement = ""),ifelse(is.null(insertion_order_id),"",paste0(", insertion_order_id: ",gsub(x = insertion_order_id, pattern = "'", replacement = ""))),", From Date: ",from_dt,", To Date: ",to_dt, (ifelse(is.null(conv_pixel_id),"",paste0(",conversion pixel_id: ",conv_pixel_id))),(ifelse(is.null(seg_pixel_id),"",paste0(",segment pixel_id: ",seg_pixel_id)))),
                   html=TRUE,
                   encoding = "utf-8",
                   smtp = list(host.name = "smtp.gmail.com", port = 465, user.name = "****", passwd = "*****", ssl = T),
                   authenticate = TRUE,
                   send =TRUE,
                   attach.file=filename)

if (file.exists(filename)) file.remove(filename)

#End of Code


