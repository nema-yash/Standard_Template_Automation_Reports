# Standard_Template_Automation_Reports(STAR Automation)

**Need for project**

1) Each Analyst(Team of 40+ analysts) spent 2-3 days running 40-100 queries creating standard post campaign reports from company advertising database.
2) Each Analyst worked on 10-15 reports a month. Leaving limited time for advanced analysis and bandwidth crunch in team
3) Amount of data is massive and analysts have to keep repeating and ensuring data pulled is correct and there is no error while analyzing data.

This process is inefficient. 

**The STAR Idea!!**

1) Found that SQL queries run were similar for most analysts and insights shown were similar too.
2) Used Google Sheet to take user inputs (like campaign id, start dates, etc.) and 
3) Created custom SQL queries in R to fetch data from the PostgreSQL database. 
4) The fetched data was processed and put in various Excel tabs before being Emailed to the user. The excel workbook consisted of 15 standard insights from Time-of-Day reports to Audience Pen portraits.

**Refer to Images "STAR Automation Architecture.png" and "STAR Automation.png" for the Solution and Standard_Template_Automation_Reports.R file for the Code**

**Impact of Solution**

Brought down delivery time of insights from 2 - 3 days to 6 minutes. 700+ insights were efficiently created using an automation initiative. 

