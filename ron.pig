REGISTER /me/Software/pig/contrib/piggybank/java/piggybank.jar
REGISTER /me/Software/pig/build/ivy/lib/Pig/joda-time-2.1.jar

DEFINE HCatLoader org.apache.hcatalog.pig.HCatLoader();
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();

/*logs = LOAD 'febweblog' using org.apache.hcatalog.pig.HCatLoader();
logs = foreach logs generate ip, user_identifier, userid, date, time, time_zone, col_7, col_8, col_9, col_10, col_11, col_12, col_13;
*/
logs = LOAD 'Feb2013.csv' using PigStorage(',') as (ip, 
                                                    user_identifier, 
                                                    userid, 
                                                    date, 
                                                    time, 
                                                    time_zone, 
                                                    col_7, 
                                                    method, 
                                                    col_9, 
                                                    col_10, 
                                                    col_11, 
                                                    col_12, 
                                                    col_13);

                         
iso_times = foreach logs GENERATE ip, 
                                 StringConcat(CustomFormatToISO(date, 'dd/MMM/YYYY'), 'T', time) as date,
                                 col_7,
                                 method,
                                 col_9,
                                 col_10,
                                 col_11,
                                 col_12,
                                 col_13;
a = limit iso_times 10;
dump a
-- 123.125.71.97,-,-,31/Jan/2013,07:15:11,-0500,GET,/robots.txt,HTTP/1.1,301,242,-,Mozilla/5.0 (Windows NT 5.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2
