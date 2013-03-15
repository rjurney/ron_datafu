REGISTER /me/Software/pig/contrib/piggybank/java/piggybank.jar
REGISTER /me/Software/pig/build/ivy/lib/Pig/joda-time-2.1.jar
REGISTER /me/Software/datafu/dist/datafu-0.0.9-SNAPSHOT.jar

DEFINE HCatLoader org.apache.hcatalog.pig.HCatLoader();
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToDay org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay();
DEFINE Sessionize datafu.pig.sessions.Sessionize('30m');

/*logs = LOAD 'febweblog' using org.apache.hcatalog.pig.HCatLoader();
logs = foreach logs generate ip, user_identifier, userid, date, time, time_zone, col_7, col_8, col_9, col_10, col_11, col_12, col_13;
*/
logs = LOAD 'Feb2013.csv' using PigStorage(',') as (ip, 
                                                    user_identifier, 
                                                    userid, 
                                                    date, 
                                                    time, 
                                                    time_zone, 
                                                    method, 
                                                    path,
                                                    protocol, 
                                                    response_code, 
                                                    response_size, 
                                                    referrer, 
                                                    user_agent);

                         
iso_times = foreach logs GENERATE StringConcat(SUBSTRING(CustomFormatToISO(date, 'dd/MMM/YYYY'), 0, 11), time) as date, 
                                  StringConcat(ip, ' ', user_agent) as user_id,
                                  path;
store iso_times into '/tmp/iso_times';
                                 
sessions = foreach (group iso_times by user_id) {
  visits = order iso_times by date;
  generate FLATTEN(Sessionize(visits)) AS (date,user_id,path,session_id);
}
a = limit sessions 10;
dump a
-- 123.125.71.97,-,-,31/Jan/2013,07:15:11,-0500,GET,/robots.txt,HTTP/1.1,301,242,-,Mozilla/5.0 (Windows NT 5.1; rv:6.0.2) Gecko/20100101 Firefox/6.0.2
