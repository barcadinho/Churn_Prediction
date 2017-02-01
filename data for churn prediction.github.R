#The purpose of the code is to obtain the subscribers who have not churned up to 15th June
#and predict who will churn in the coming month (up to 15th July)

#save image
save.image(file="data.for.churn.prediction.RData")

#load
load(file="data.for.churn.prediction.RData")

library(RPostgreSQL)
library(dplyr)
library(ggplot2)
library(scales)
library(stargazer)
library(sqldf)
library(pglm)
library(survival)
library(pscl)
library(data.table)


driver = dbDriver("PostgreSQL")

pw<-"To be filled in"

#create con, store the password in a variable, don't leave it on this script
con = dbConnect(driver, dbname = "reporting",
                host = "localhost", port = 5439,
                user = "product", password = pw)

#******************data untill 20th Aug************************



#data cleaning
#I first select a list of users who have not churned up to 15th June
#and then acquire their behavior  on Viki, some examples including viewing, subtitling, reviews etc.



#User profile of subscribers who have not churned up to 15th June
user.profile<-dbGetQuery(con,"select id as user_id,email,username,name,gender, created_at as regdate,qc,staff,editor,email_post,email_video,email_message,
                            email_comment,email_newsletter,last_country,birth_date_d,registration_method,
                            content_language,subtitle_language from reporting.users a, (
                            select distinct user_id from vikipass.plan_subscribers where end_date::date>'2016-06-15'
                            union
                            select distinct user_id from vikipass.viki_subscriptions where end_date::date>'2016-06-15') b
                            where a.staff='false' and a.id=b.user_id")

#Whether a container is a blockbuster show or not
#the definition of "blockbuster" is based on total number of views
container<-dbGetQuery(con,"select container_id,count(*)>5000000 as blockbuster from hadoop.user_video_sessions group by container_id")
container$blockbuster<-as.numeric(container$blockbuster)



#segment contribution (daily)
user.segments<-dbGetQuery(con,"SELECT user_id|| 'u' AS user_id,
                          created_at::DATE AS DATE,
                          COUNT(*) as num_segments
                          FROM subber.segments
                          where created_at::DATE>='2014-05-01' GROUP BY user_id,
                          created_at::DATE")
#subtitle contribution (daily)
user.subtitles<-dbGetQuery(con,"SELECT user_id|| 'u' AS user_id,
                           created_at::DATE AS DATE,
                           COUNT(*) as num_subtitles
                           FROM subber.subtitles
                           where created_at::DATE>='2014-05-01' GROUP BY user_id,
                           created_at::DATE")

#sending and receiving message (daily)
user.pm<-dbGetQuery(con,"SELECT sender_id as user_id,
                    created_at::DATE AS DATE,
                    COUNT(DISTINCT content) as num_threads,
                    COUNT(*) as num_recepients
                    FROM aphrodite.threads
                    where created_at::DATE>='2014-05-01' GROUP BY sender_id,
                    created_at::DATE")

#reviews (daily)
user.review<-dbGetQuery(con,"SELECT user_id,
                        created_at::DATE AS DATE,
                        COUNT(*) as num_reviews
                        FROM oceanus.reviews
                        where created_at::DATE>='2014-05-01' GROUP BY user_id,
                        created_at::DATE")


#number of timed_comments (daily)
user.timed_comments<-dbGetQuery(con,"SELECT user_id,
                                created_at::DATE AS DATE,
                                COUNT(*) as num_timed_comments
                                FROM oceanus.timed_comments
                                where created_at::DATE>='2014-05-01' GROUP BY user_id,
                                created_at::DATE")

#number of disqus_comments (daily)
user.disqus_comments<-dbGetQuery(con,"SELECT b.id as user_id, a.created_at as date_d, count(*) as num_disqus_comments
                                 FROM api_imports.disqus_email a,
                                 reporting.users b
                                 WHERE a.author_email = b.email
                                 and a.created_at >= '2014-05-01'
                                 GROUP BY b.id,
                                 a.created_at")
#Follow activities (daily)
user.follow<-dbGetQuery(con,"select user_id,created_at::date as date_d, count(*) as num_follow from gaia.subscriptions 
                        where created_at::DATE>='2014-05-01'  group by user_id,created_at::date")


#Merge all the behaviors above into a single table, based on user_id and date
user.regbehave<-merge(user.pm,user.review,by.x=c("user_id","date"),by.y=c("user_id","date"),all.x = TRUE,all.y = TRUE)
user.regbehave<-merge(user.regbehave,user.segments,by.x=c("user_id","date"),by.y=c("user_id","date"),all.x = TRUE,all.y = TRUE)
user.regbehave<-merge(user.regbehave,user.subtitles,by.x=c("user_id","date"),by.y=c("user_id","date"),all.x = TRUE,all.y = TRUE)
user.regbehave<-merge(user.regbehave,user.disqus_comments,by.x=c("user_id","date"),by.y=c("user_id","date_d"),all.x = TRUE,all.y = TRUE)
user.regbehave<-merge(user.regbehave,user.follow,by.x=c("user_id","date"),by.y=c("user_id","date_d"),all.x = TRUE,all.y = TRUE)
user.regbehave<-merge(user.regbehave,user.timed_comments,by.x=c("user_id","date"),by.y=c("user_id","date"),all.x = TRUE,all.y = TRUE)


#I only need their behavior after 2016-05-01
user.regbehave<-user.regbehave[which(user.regbehave$date>=as.Date('2016-05-01')),]

#set NA as zero
user.regbehave[is.na(user.regbehave)]<-0

#Daily viewing behavior for those with user_id ending with (1u|2u|3U) from 1st May
#I do this because of limited computational power of my personal computer
user_viewing<-dbGetQuery(con,"SELECT c.date_d,
                                  c.as_id,
                                  c.user_id as login_user,
                                  d.user_id,
                                  c.app_id,
                                  c.stream_quality,
                                  c.container_id,
                                  count(distinct video_id) as num_video_distinct,
                                  sum(video_load_cnt) as num_video_loaded,
                                  SUM(video_play_cnt) AS num_video_played,
                                  SUM(video_blocked_cnt) AS num_video_blocked,
                                  SUM(minute_view_cnt) AS num_play_minutes,
                                  SUM(ad_started_cnt) AS num_ads,
                                  SUM(pre_roll_ad_cnt) AS num_ads_pre,
                                  SUM(mid_roll_ad_cnt) AS num_ads_mid,
                                  SUM(exit_roll_ad_cnt) AS num_ads_exit,
                                  SUM(ad_click_cnt) AS num_ads_click
                                  FROM hadoop.user_video_sessions c
                                  INNER JOIN (SELECT DISTINCT a.user_id,
                                  a.uuid
                                  FROM hadoop.uuid_user_refs a,
                                  (SELECT DISTINCT user_id
                                  FROM vikipass.plan_subscribers
                                  WHERE end_date::DATE> '2016-06-15' AND   user_id~'.*[1|2|3]u'
                                  UNION
                                  SELECT DISTINCT user_id
                                  FROM vikipass.viki_subscriptions
                                  WHERE end_date::DATE> '2016-06-15' AND   user_id~'.*[1|2|3]u') b
                                  WHERE a.user_id = b.user_id
                                  AND   a.date_d >= '2016-05-01') d
                                  ON c.uuid = d.uuid
                                  AND c.date_d >= '2016-05-01'
                                  GROUP BY c.date_d,
                                  c.as_id,
                                  c.user_id,
                                  d.user_id,
                                  c.uuid,
                                  c.app_id,
                                  c.stream_quality,
                                  c.container_id
                                  ")

#user profile for users with 1|2|3 u
user.profile<-user.profile[which(grepl('.*[1|2|3]u',user.profile$user_id)==TRUE),]




#load subscription period from another RData file
"***************to be added********************"
sub_period$yearly.plan<-as.integer(sub_period$viki_plan_id %in% c("19p","12p","22p","24p","25p"))

#get the last subscription period of the subscribers who have not churned up to 15th June
user.0615<-sub_period[which(as.Date(sub_period$start_date+8*3600)<as.Date('2016-06-15') & as.Date(sub_period$end_date+8*3600)>=as.Date('2016-06-15')),]

#Length of the last subscription period
user.0615$sub.days<-as.integer(as.Date('2016-06-15')-as.Date(user.0615$start_date+8*3600))


#whether the subscriber churned before 15th July
#the dependent variable we want to predict
user.0615$churn.before.0715<-as.integer(as.Date(user.0615$end_date+8*3600)<as.Date('2016-07-15'))


#get the total number of previous subscriptions before 15th June
temp<-sub_period[which(as.Date(sub_period$end_date+8*3600)<as.Date('2016-06-15')),]%>%group_by(user_id)%>%summarise(prev.sub=n())
user.0615<-merge(user.0615,temp,by.x = "user_id",by.y = "user_id",all.x = T,all.y = F)
user.0615[is.na(user.0615$prev.sub),"prev.sub"]<-0




#still, whi only need those with user_id ending with 1|2|3u
#merge the subscription period table with user profile table
user.0615.randsamp<-merge(user.profile,user.0615,by.x = "user_id",by.y = "user_id",all.x = F,all.y = F)

rm(user.0615)

#remove the yearly plan subscribers
user.0615.randsamp<-user.0615.randsamp[which(user.0615.randsamp$yearly.plan==0),]

#registration days
user.0615.randsamp$reg.days<-as.integer(difftime(as.Date('2016-06-15'), as.Date(user.0615.randsamp$regdate+8*3600),
                                       units = "days"))

#transform age and gender into dummy variables
user.0615.randsamp[which(user.0615.randsamp$gender=='' | is.na(user.0615.randsamp$gender)),"gender"]<-"unknown"
user.0615.randsamp$num.age<-ceiling(as.numeric(Sys.Date()-user.0615.randsamp$birth_date_d)/365)
user.0615.randsamp[which(user.0615.randsamp$num.age<=10),"age"]<-"0-10"
user.0615.randsamp[which(user.0615.randsamp$num.age>10 & user.0615.randsamp$num.age<=20),"age"]<-"10-20"
user.0615.randsamp[which(user.0615.randsamp$num.age>20 & user.0615.randsamp$num.age<=30),"age"]<-"20-30"
user.0615.randsamp[which(user.0615.randsamp$num.age>30 & user.0615.randsamp$num.age<=40),"age"]<-"30-40"
user.0615.randsamp[which(user.0615.randsamp$num.age>40 & user.0615.randsamp$num.age<=50),"age"]<-"40-50"
user.0615.randsamp[which(user.0615.randsamp$num.age>50),"age"]<-"50-inf"
user.0615.randsamp[is.na(user.0615.randsamp$num.age),"age"]<-"unknown"
user.0615.randsamp$gender<-as.factor(user.0615.randsamp$gender)
user.0615.randsamp$gender<-relevel(user.0615.randsamp$gender,ref="unknown")
user.0615.randsamp$age<-as.factor(user.0615.randsamp$age)
user.0615.randsamp$age<-relevel(user.0615.randsamp$age,ref="unknown")




#video viewing behavior from may to june
user_viewing_may2jun<-user_viewing[which(user_viewing$date_d>=as.Date("2016-05-15") & user_viewing$date_d<=as.Date("2016-06-15")),]

#whether exclusive show or not
user_viewing_may2jun$show.exclusive<-as.integer(user_viewing_may2jun$container_id %in% c('28665c','29149c','30854c','31137c'))


user_viewing_may2jun$binge.watching<-as.integer(user_viewing_may2jun$num_video_distinct>=3)

user_viewing_may2jun$HD<-as.integer(user_viewing_may2jun$stream_quality=='720p')

#whether the session is within subscription period or not
user_viewing_may2jun<-merge(user_viewing_may2jun,user.0615.randsamp[,c("user_id","start_date","end_date")],by.x = "user_id",by.y = "user_id",all.x = T,all.y = F)

user_viewing_may2jun$within.subprd<-as.integer(user_viewing_may2jun$date_d>=as.Date(user_viewing_may2jun$start_date) & user_viewing_may2jun$date_d<=as.Date(user_viewing_may2jun$end_date))

#choose only the observations within subscription period 
user_viewing_may2jun<-user_viewing_may2jun[which(user_viewing_may2jun$within.subprd==1),]
user_viewing_may2jun<-merge(user_viewing_may2jun,container,by.x="container_id",by.y="container_id",all.x=T,all.y=F)


#monthly summary of video viewing behavior
#I use data.table package because it is computationally much faster
library(data.table)
user_viewing_may2jun<-as.data.table(user_viewing_may2jun)

user_viewing_may2jun_sessionlevel<-user_viewing_may2jun[,j=list(date_d=max(date_d),
                                                                                   tot.login=sum(as.numeric(login_user!='' & !is.na(login_user))*num_play_minutes),
                                                                                   tot.blockbuster=sum(blockbuster*num_play_minutes),
                                                                                   binge.watching=max(binge.watching),
                                                                                   tot.view.exclusive=sum(show.exclusive*num_play_minutes),
                                                                                   android=max(app_id=="100005a"),
                                                                                   ios=max(app_id=="100004a"),
                                                                                   web=max(app_id %in% c("65535a","100000a")),
                                                                                   TV=max(app_id=="100015a"),
                                                                                   tot.HD=sum(HD*num_play_minutes),
                                                                                   view.time.tot=sum(num_play_minutes),
                                                                                   num.shows.tot=.N,
                                                                                   num.videos.tot=sum(num_video_played),
                                                                                   num.videos.blocked=sum(num_video_blocked),
                                                                                   ads.tot=sum(num_ads),
                                                                                   ads.pre.tot=sum(num_ads_pre),
                                                                                   ads.mid.tot=sum(num_ads_mid),
                                                                                   ads.click.tot=sum(num_ads_click)),by=list(user_id,as_id)]


user_viewing_may2jun_summary<-user_viewing_may2jun_sessionlevel[,j=list(visit.freq=.N/as.integer(max(date_d)-min(date_d)+1),
                                                                                   percent.blockbuster=sum(tot.blockbuster)/(sum(view.time.tot)+1),
                                                                                   percent.login=sum(tot.login)/(sum(view.time.tot)+1),
                                                                                   percent.binge.watching=sum(binge.watching*view.time.tot)/(sum(view.time.tot)+1),
                                                                                   percent.view.exclusive=sum(tot.view.exclusive)/(sum(view.time.tot)+1),
                                                                                   percent.android=sum(android*view.time.tot)/(sum(view.time.tot)+1),
                                                                                   percent.ios=sum(ios*view.time.tot)/(sum(view.time.tot)+1),
                                                                                   percent.web=sum(web*view.time.tot)/(sum(view.time.tot)+1),
                                                                                   percent.TV=sum(TV*num.shows.tot)/(sum(num.shows.tot)+1),
                                                                                   percent.HD=sum(tot.HD)/(sum(view.time.tot)+1),
                                                                                   view.time.avg=sum(view.time.tot)/.N,
                                                                                   num.shows.avg=sum(num.shows.tot)/.N,
                                                                                   num.videos.avg=sum(num.videos.tot)/.N,
                                                                                   num.videos.blocked.avg=sum(num.videos.blocked)/.N,
                                                                                   ads.avg=sum(ads.tot)/(sum(view.time.tot-tot.login)+1),
                                                                                   ads.pre.avg=sum(ads.pre.tot)/(sum(view.time.tot-tot.login)+1),
                                                                                   ads.mid.avg=sum(ads.mid.tot)/(sum(view.time.tot-tot.login)+1),
                                                                                   ads.click.avg=sum(ads.click.tot)/(sum(view.time.tot-tot.login)+1)),by=user_id]


#*********************video viewing behavior 7 days before 15th June
user_viewing_7daysbefore<-user_viewing[which(user_viewing$date_d>=as.Date("2016-06-08") & user_viewing$date_d<=as.Date("2016-06-15")),]

#whether exclusive show or not
user_viewing_7daysbefore$show.exclusive<-as.integer(user_viewing_7daysbefore$container_id %in% c('28665c','29149c','30854c','31137c'))


user_viewing_7daysbefore$binge.watching<-as.integer(user_viewing_7daysbefore$num_video_distinct>=3)

user_viewing_7daysbefore$HD<-as.integer(user_viewing_7daysbefore$stream_quality=='720p')

#whether the session is within subscription period or not
user_viewing_7daysbefore<-merge(user_viewing_7daysbefore,user.0615.randsamp[,c("user_id","start_date","end_date")],by.x = "user_id",by.y = "user_id",all.x = T,all.y = F)

user_viewing_7daysbefore$within.subprd<-as.integer(user_viewing_7daysbefore$date_d>=as.Date(user_viewing_7daysbefore$start_date) & user_viewing_7daysbefore$date_d<=as.Date(user_viewing_7daysbefore$end_date))

#choose only the within subscription period observations
user_viewing_7daysbefore<-user_viewing_7daysbefore[which(user_viewing_7daysbefore$within.subprd==1),]
user_viewing_7daysbefore<-merge(user_viewing_7daysbefore,container,by.x="container_id",by.y="container_id",all.x=T,all.y=F)


#monthly summary of video viewing behavior
library(data.table)
user_viewing_7daysbefore<-as.data.table(user_viewing_7daysbefore)

user_viewing_7daysbefore_sessionlevel<-user_viewing_7daysbefore[,j=list(date_d=max(date_d),
                                                                tot.login=sum(as.numeric(login_user!='' & !is.na(login_user))*num_play_minutes),
                                                                tot.blockbuster=sum(blockbuster*num_play_minutes),
                                                                binge.watching=max(binge.watching),
                                                                tot.view.exclusive=sum(show.exclusive*num_play_minutes),
                                                                android=max(app_id=="100005a"),
                                                                ios=max(app_id=="100004a"),
                                                                web=max(app_id %in% c("65535a","100000a")),
                                                                TV=max(app_id=="100015a"),
                                                                tot.HD=sum(HD*num_play_minutes),
                                                                view.time.tot=sum(num_play_minutes),
                                                                num.shows.tot=.N,
                                                                num.videos.tot=sum(num_video_played),
                                                                num.videos.blocked=sum(num_video_blocked),
                                                                ads.tot=sum(num_ads),
                                                                ads.pre.tot=sum(num_ads_pre),
                                                                ads.mid.tot=sum(num_ads_mid),
                                                                ads.click.tot=sum(num_ads_click)),by=list(user_id,as_id)]


user_viewing_7daysbefore_summary<-user_viewing_7daysbefore_sessionlevel[,j=list(visit.freq.7daysbefore=.N/as.integer(max(date_d)-min(date_d)+1),
                                                                        percent.blockbuster.7daysbefore=sum(tot.blockbuster)/(sum(view.time.tot)+1),
                                                                        percent.login.7daysbefore=sum(tot.login)/(sum(view.time.tot)+1),
                                                                        percent.binge.watching.7daysbefore=sum(binge.watching*view.time.tot)/(sum(view.time.tot)+1),
                                                                        percent.view.exclusive.7daysbefore=sum(tot.view.exclusive)/(sum(view.time.tot)+1),
                                                                        percent.android.7daysbefore=sum(android*view.time.tot)/(sum(view.time.tot)+1),
                                                                        percent.ios.7daysbefore=sum(ios*view.time.tot)/(sum(view.time.tot)+1),
                                                                        percent.web.7daysbefore=sum(web*view.time.tot)/(sum(view.time.tot)+1),
                                                                        percent.TV.7daysbefore=sum(TV*num.shows.tot)/(sum(num.shows.tot)+1),
                                                                        percent.HD.7daysbefore=sum(tot.HD)/(sum(view.time.tot)+1),
                                                                        view.time.avg.7daysbefore=sum(view.time.tot)/.N,
                                                                        num.shows.avg.7daysbefore=sum(num.shows.tot)/.N,
                                                                        num.videos.avg.7daysbefore=sum(num.videos.tot)/.N,
                                                                        num.videos.blocked.avg.7daysbefore=sum(num.videos.blocked)/.N,
                                                                        ads.avg.7daysbefore=sum(ads.tot)/(sum(view.time.tot-tot.login)+1),
                                                                        ads.pre.avg.7daysbefore=sum(ads.pre.tot)/(sum(view.time.tot-tot.login)+1),
                                                                        ads.mid.avg.7daysbefore=sum(ads.mid.tot)/(sum(view.time.tot-tot.login)+1),
                                                                        ads.click.avg.7daysbefore=sum(ads.click.tot)/(sum(view.time.tot-tot.login)+1)),by=user_id]




























#registration behavior from 15th May to 15th June 
user.regbehave_may2jun<-user.regbehave[which(user.regbehave$date>=as.Date("2016-05-15") & user.regbehave$date<=as.Date("2016-06-15")),]
#whether the session is within subscription period or not
user.regbehave_may2jun<-merge(user.regbehave_may2jun,user.0615.randsamp[,c("user_id","start_date","end_date")],by.x = "user_id",by.y = "user_id",all.x = T,all.y = F)

user.regbehave_may2jun$within.subprd<-as.integer(user.regbehave_may2jun$date>=as.Date(user.regbehave_may2jun$start_date) & user.regbehave_may2jun$date<=as.Date(user.regbehave_may2jun$end_date))

#choose only the within subscription period observations
user.regbehave_may2jun<-user.regbehave_may2jun[which(user.regbehave_may2jun$within.subprd==1),]

#summary
user.regbehave_may2jun_summary<-user.regbehave_may2jun%>%group_by(user_id)%>%summarise(avg.threads=sum(num_threads)/(sum(as.integer(num_threads>0))+1),
                                                                                       avg.reviews=sum(num_reviews)/(sum(as.integer(num_reviews>0))+1),
                                                                                       avg.segments=sum(num_segments)/(sum(as.integer(num_segments>0))+1),
                                                                                       avg.subtitles=sum(num_subtitles)/(sum(as.integer(num_subtitles>0))+1),
                                                                                       avg.disqus_comments=sum(num_disqus_comments)/(sum(as.integer(num_disqus_comments>0))+1),
                                                                                       avg.timed_comments=sum(num_timed_comments)/(sum(as.integer(num_timed_comments>0))+1),
                                                                                       avg.follow=sum(num_follow)/(sum(as.integer(num_follow>0))+1))




#merge viewing behavior with user profile data
user.0615.merge<-merge(user.0615.randsamp,user_viewing_may2jun_summary,by.x = "user_id",by.y = "user_id",all.x = T,all.y = F)
user.0615.merge<-merge(user.0615.merge,user_viewing_7daysbefore_summary,by.x = "user_id",by.y = "user_id",all.x = T,all.y = F)
user.0615.merge<-merge(user.0615.merge,user.regbehave_may2jun_summary,by.x = "user_id",by.y = "user_id",all.x = T,all.y = F)
user.0615.merge[, 33:75][is.na(user.0615.merge[, 33:75])] <-0

#change of behavior
user.0615.merge$visit.freq.change<-(user.0615.merge$visit.freq.7daysbefore-user.0615.merge$visit.freq)/(user.0615.merge$visit.freq+0.01)
user.0615.merge$percent.android.change<-(user.0615.merge$percent.android.7daysbefore-user.0615.merge$percent.android)/(user.0615.merge$percent.android+0.01)
user.0615.merge$percent.ios.change<-(user.0615.merge$percent.ios.7daysbefore-user.0615.merge$percent.ios)/(user.0615.merge$percent.ios+0.01)
user.0615.merge$percent.web.change<-(user.0615.merge$percent.web.7daysbefore-user.0615.merge$percent.web)/(user.0615.merge$percent.web+0.01)
user.0615.merge$percent.TV.change<-(user.0615.merge$percent.TV.7daysbefore-user.0615.merge$percent.TV)/(user.0615.merge$percent.TV+0.01)
user.0615.merge$percent.HD.change<-(user.0615.merge$percent.HD.7daysbefore-user.0615.merge$percent.HD)/(user.0615.merge$percent.HD+0.01)
user.0615.merge$percent.blockbuster.change<-(user.0615.merge$percent.blockbuster.7daysbefore-user.0615.merge$percent.blockbuster)/(user.0615.merge$percent.blockbuster+0.01)
user.0615.merge$percent.login.change<-(user.0615.merge$percent.login.7daysbefore-user.0615.merge$percent.login)/(user.0615.merge$percent.login+0.01)
user.0615.merge$percent.binge.watching.change<-(user.0615.merge$percent.binge.watching.7daysbefore-user.0615.merge$percent.binge.watching)/(user.0615.merge$percent.binge.watching+0.01)
user.0615.merge$percent.view.exclusive.change<-(user.0615.merge$percent.view.exclusive.7daysbefore-user.0615.merge$percent.view.exclusive)/(user.0615.merge$percent.view.exclusive+0.01)
user.0615.merge$view.time.avg.change<-(user.0615.merge$view.time.avg.7daysbefore-user.0615.merge$view.time.avg)/(user.0615.merge$view.time.avg+0.01)
user.0615.merge$num.shows.avg.change<-(user.0615.merge$num.shows.avg.7daysbefore-user.0615.merge$num.shows.avg)/(user.0615.merge$num.shows.avg+0.01)
user.0615.merge$num.videos.blocked.avg.change<-(user.0615.merge$num.videos.blocked.avg.7daysbefore-user.0615.merge$num.videos.blocked.avg)/(user.0615.merge$num.videos.blocked.avg+0.01)
user.0615.merge$ads.avg.change<-(user.0615.merge$ads.avg.7daysbefore-user.0615.merge$ads.avg)/(user.0615.merge$ads.avg+0.01)
user.0615.merge$ads.click.avg.change<-(user.0615.merge$ads.click.avg.7daysbefore-user.0615.merge$ads.click.avg)/(user.0615.merge$ads.click.avg+0.01)


user.0615.merge[!(user.0615.merge$last_country %in% c("gb","fr","us","it","kr","mx","sg","ca")),"last_country"]<-"others"
user.0615.merge$last_country<-as.factor(user.0615.merge$last_country)
user.0615.merge$last_country<-relevel(user.0615.merge$last_country,ref="others")

user.0615.merge$start_month<-format(user.0615.merge$start_date,"%Y%m")



#prediction
library(caret)

overall<- user.0615.merge[which(as.Date(user.0615.merge$start_date)>as.Date("2016-05-01")),]
overall$already.churned<-I(format(overall$cancel_date,"%Y%m%d")<='20160615')
overall[is.na(overall$already.churned),"already.churned"]<-FALSE

pred<-data.frame(`0`=numeric(),`1`=numeric(),user_id=character(),churn.before.0715=numeric())

#10 fold prediction
for (i in 0:9)
{
test = overall[(floor(0.1*i*nrow(overall))+1):floor(0.1*(i+1)*nrow(overall)),]
training = overall[-((floor(0.1*i*nrow(overall))+1):floor(0.1*(i+1)*nrow(overall))),]


mod_fit = train(as.factor(churn.before.0715)~prev.sub+sub.days+reg.days+visit.freq+percent.ios+percent.android+percent.web+percent.HD+
                  percent.login+percent.binge.watching+percent.view.exclusive+already.churned*sub.method+
                  view.time.avg+num.shows.avg+num.videos.blocked.avg+
                  ads.avg+ads.click.avg+avg.reviews+avg.timed_comments+avg.threads+
                  avg.segments+avg.subtitles+avg.disqus_comments+avg.follow+
                  visit.freq.change+percent.ios.change+percent.android.change+percent.web.change+percent.HD.change+
                  percent.login.change+percent.binge.watching.change+percent.view.exclusive.change+view.time.avg.change+num.shows.avg.change+num.videos.blocked.avg.change+
                  ads.avg.change+ads.click.avg.change
                  ,data=training,method="glm",family="binomial")


#For prediction
temp = predict(mod_fit, newdata=test,type="prob",na.action = na.pass)
temp<-cbind(temp,test[,c("user_id","churn.before.0715")])
pred<-rbind(temp,pred)
print(i)
}

#the user is predicted to churn if the predicted probability is larger than the proportion of churners
#(we could also use equal sample size and 50% criteria for churn prediction)
pred$predict.churn = as.numeric(pred[,"1"]>=nrow(overall[which(overall$churn.before.0715==1),])/nrow(overall))
accuracy <- table(pred[,c("predict.churn","churn.before.0715")])
sum(diag(accuracy))/sum(accuracy)
accuracy
recall<-accuracy[2,2]/(accuracy[2,2]+accuracy[1,2])
precision<-accuracy[2,2]/(accuracy[2,2]+accuracy[2,1])
F1<-2*precision*recall/(precision+recall)