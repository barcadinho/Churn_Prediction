############This code is to get the subscription period and canceling date of each user#######################


#save workspace
save.image(file="user.sub.cancel.date.RData")

#load workspace
load(file="user.sub.cancel.date.RData")



library(RPostgreSQL)
library(dplyr)
library(ggplot2)
library(scales)
library(stargazer)
library(sqldf)


driver = dbDriver("PostgreSQL")

pw<-"p9dh6uTzd4wB3kpr"

#create con, store the password in a variable, don't leave it on this script
con = dbConnect(driver, dbname = "reporting",
                host = "localhost", port = 5439,
                user = "product", password = pw)


#for viki_subscription_activities table, activity of "cancel" represent canceling activity
#for viki_subscription_activities the observation with status of "active" and "canceled_at" not null
#represents canceling activity.
#there is another case with the previous observation of "past_due" and the following observation of 
#"canceled", I think this case represents failure of charging.
#for web subscriptions, resubscription is recorded in the viki_subscriptions table (actually not
#all of them, example: 11688364u)
#for mobile subscriptions, if there is resubscription, the plan_subscribers table only record the 
#latest subscription period


#determine the subscription period of mobile phone users
#the idea is that I firstly determine which observation determines the start and end of a subscription period
#for this part I use status='trialing'/'canceling' or lagged status='canceling' or the last row of a subscription period as indicators.
#Then I select the start date and the end date


mob_subprd<-dbGetQuery(con,"SELECT user_id,
       viki_plan_id,
                       start_date,
                       (CASE WHEN (end_date_inactive < end_date_active or end_date_inactive is null) THEN end_date_active ELSE end_date_inactive END) AS end_date
                       FROM (SELECT user_id,
                       viki_plan_id,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at ASC),
                       created_at AS start_date,
                       (case  when (LEAD(end_date,1) OVER (PARTITION BY user_id ORDER BY created_at ASC)) is null then end_date else 
                       (LEAD(end_date,1) OVER (PARTITION BY user_id ORDER BY created_at ASC)) end)AS end_date_active,
                       LEAD(created_at,1) OVER (PARTITION BY user_id ORDER BY created_at ASC) AS end_date_inactive
                       FROM (SELECT *
                       FROM (SELECT user_id,
                       viki_plan_id,
                       status,
                       LAG(status,1) OVER (PARTITION BY user_id ORDER BY created_at ASC) = 'ended' AND (status = 'active' or status='trialling') AS resubscription,
                       LEAD(status,1) OVER (PARTITION BY user_id ORDER BY created_at ASC) IS NULL AS lastrow,
                       LAG(status,1) OVER (PARTITION BY user_id ORDER BY created_at ASC) IS NULL AS firstrow,
                       start_date,
                       end_date,
                       created_at
                       FROM vikipass.plan_subscriber_activities_v2
                       WHERE payment_provider != 'stripe'
                       ORDER BY user_id,
                       created_at ASC) AS t
                       WHERE status = 'ended'
                       OR    firstrow = TRUE
                       OR    resubscription = TRUE
                       OR    lastrow = TRUE) AS t1) AS t2
                       WHERE ROW_NUMBER % 2 = 1")

#most of the cases with null end_date are nonsense cases
mob_subprd<-mob_subprd[!is.na(mob_subprd$end_date),]
mob_subprd$start_date<-as.character(mob_subprd$start_date)
mob_subprd$end_date<-as.character(mob_subprd$end_date)
mob_subprd$start_date<-as.POSIXct(mob_subprd$start_date,"%Y-%m-%d %H:%M:%S",tz='GMT')
mob_subprd$end_date<-as.POSIXct(mob_subprd$end_date,"%Y-%m-%d %H:%M:%S",tz='GMT')


#web subscription inclusing both active canceling and canceling because of payment failure
web_subprd_vs<-dbGetQuery(con,"SELECT user_id,
       viki_plan_id,
                          viki_subscription_id,
                          vs_start_date AS start_date,
                          MAX(current_end_date) AS end_date,
                          MAX(CASE WHEN activity = 'cancel' and lead_status='ended' THEN created_at WHEN activity = 'end' AND lag_status = 'trialling' AND lag_current_end_date > current_end_date THEN created_at ELSE NULL END) AS cancel_date
                          FROM (SELECT viki_subscription_id,
                          viki_plan_id,
                          activity,
                          status,
                          lag_status,
                          lead_status,
                          lag_current_end_date,
                          (CASE WHEN user_id = '' THEN LAG(user_id,1) OVER (PARTITION BY viki_subscription_id ORDER BY created_at ASC) ELSE user_id END) AS user_id,
                          (CASE WHEN vs_start_date = '{}' THEN LAG(vs_start_date,1) OVER (PARTITION BY viki_subscription_id ORDER BY created_at ASC) ELSE vs_start_date END) AS vs_start_date,
                          (CASE WHEN current_end_date = '{}' THEN '2016-11-12 09:07:18' ELSE current_end_date END) AS current_end_date,
                          created_at
                          FROM (SELECT viki_subscription_id,
                          viki_plan_id,
                          activity,
                          status,
                          regexp_substr(payload,'[0-9]{1,10}u') AS user_id,
                          REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(payload,'.*created_at.{4}'),'.[0-9]{3}Z.*'),'T',' ') AS vs_start_date,
                          REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(payload,'.*end_date.{4}'),'.[0-9]{3}Z.*'),'T',' ') AS current_end_date,
                          (LAG(status,1) OVER (PARTITION BY viki_subscription_id ORDER BY created_at ASC)) AS lag_status,
                          (lead(status,1) OVER (PARTITION BY viki_subscription_id ORDER BY created_at ASC)) AS lead_status,
                          (LAG(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(payload,'.*end_date.{4}'),'.[0-9]{3}Z.*'),'T',' '),1) OVER (PARTITION BY viki_subscription_id ORDER BY created_at ASC)) AS lag_current_end_date,
                          created_at
                          FROM vikipass.viki_subscription_activities
                          WHERE CHAR_LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(payload,'.*end_date.{4}'),'.[0-9]{3}Z.*'),'T',' ')) <= 20
                          ORDER BY regexp_substr(payload,'[0-9]{1,10}u'),
                          created_at))
                          GROUP BY user_id,
                          viki_plan_id,
                          viki_subscription_id,
                          vs_start_date")

web_subprd_vs$start_date<-as.POSIXct(web_subprd_vs$start_date,"%Y-%m-%d %H:%M:%S",tz='GMT')
web_subprd_vs$end_date<-as.POSIXct(web_subprd_vs$end_date,"%Y-%m-%d %H:%M:%S",tz='GMT')

web_subprd_vs$cancel_date<-as.character(web_subprd_vs$cancel_date)
web_subprd_vs$cancel_date<-as.POSIXct(web_subprd_vs$cancel_date,"%Y-%m-%d %H:%M:%S",tz='GMT')



#note that this part does not change at all, so do not need to re-run the code when updating subscription period
web_subprd_ps<-dbGetQuery(con,"SELECT user_id,viki_plan_id,
       start AS start_date,
                          MAX(end_date) AS end_date,
                          MAX(cancel_date) AS cancel_date
                          FROM (SELECT user_id,viki_plan_id,
                          start,
                          (CASE LAST_VALUE(status) OVER (PARTITION BY user_id,start ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) WHEN 'past_due' THEN current_period_start ELSE current_period_end END) AS end_date,
                          canceled_at AS cancel_date
                          FROM vikipass.plan_subscriber_activities
                          WHERE payment_provider = 'stripe'
                          AND   status != 'canceled'
                          ORDER BY user_id,
                          start,
                          created_at) AS t
                          GROUP BY user_id,viki_plan_id,
                          start")

web_subprd_ps$start_date<-as.character(web_subprd_ps$start_date)
web_subprd_ps$end_date<-as.character(web_subprd_ps$end_date)
web_subprd_ps$cancel_date<-as.character(web_subprd_ps$cancel_date)

web_subprd_ps$start_date<-as.POSIXct(web_subprd_ps$start_date,"%Y-%m-%d %H:%M:%S",tz='GMT')
web_subprd_ps$end_date<-as.POSIXct(web_subprd_ps$end_date,"%Y-%m-%d %H:%M:%S",tz='GMT')
web_subprd_ps$cancel_date<-as.POSIXct(web_subprd_ps$cancel_date,"%Y-%m-%d %H:%M:%S",tz='GMT')



web_subprd_merge<-rbind(web_subprd_ps[,1:5],web_subprd_vs[,c("user_id","viki_plan_id","start_date","end_date","cancel_date")])

#remove duplication
attach(web_subprd_merge)
web_subprd_merge<-web_subprd_merge[order(user_id,start_date,-as.numeric(end_date),-as.numeric(cancel_date)),]
detach(web_subprd_merge)
web_subprd_merge$start_date.d<-as.Date(web_subprd_merge$start_date)
web_subprd_merge<-web_subprd_merge[!duplicated(web_subprd_merge[c("user_id","start_date.d")]),]




#web subscription duration
web_subprd_merge$sub_duration<-as.Date(web_subprd_merge$end_date+8*3600)-as.Date(web_subprd_merge$start_date+8*3600)

web_subprd_merge$sub.method<-"web"

#mob subscription duration
mob_subprd$sub_duration<-as.Date(mob_subprd$end_date+8*3600)-as.Date(mob_subprd$start_date+8*3600)

mob_subprd$sub.method<-"mobile"
#subsscription for mob and web
sub_period<-rbind(mob_subprd[,c("user_id","viki_plan_id","start_date","end_date","sub_duration","sub.method")],
                  web_subprd_merge[,c("user_id","viki_plan_id","start_date","end_date","sub_duration","sub.method")])

#get the cancelling date of web subscription
sub_period<-merge(sub_period,web_subprd_merge[,c("user_id","start_date","cancel_date")],by.x = c("user_id","start_date"),by.y = c("user_id","start_date"), all.x = T,all.y = F)


sub_period<-sub_period[order(sub_period$user_id,sub_period$end_date),]%>%group_by(user_id)%>%mutate(rank=row_number())

viki.plans<-dbGetQuery(con,"select * from vikipass.viki_plans")

sub_period<-merge(sub_period,viki.plans[,c("id","credit")],by.x = "viki_plan_id",by.y = "id",all.x = T,all.y = F)



save(sub_period,file = "sub_period.RData")

#save workspace
save.image(file="user.sub.cancel.date.RData")