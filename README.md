# Connection Testing Script

This project came out from my need to constantly monitor my wan connection when my family is complaining about internet issues in the house.

The script simply pings given list of ip addresses with some delay for a given number of times, and then prints the results. It also writes the result into tsdb.

The script can be added to cron to be run periodically. The results from tsdb can then be graphed using grafana.

In my case, I run the script in a way that it runs each time for less than a minute (9 times, 4 sec delays). Then I have a cron that runs every minute to executes the script. My tsdb and grafana (in docker) shows me a full picture of packet loss and rtt for the whole day, min by min.


