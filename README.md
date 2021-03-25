Problem Statement :  Spatially Analyze the COVID Transmission data and perform root cause analysis. The address data is a free text field and throws about 30% error when plotted on GIS Map or Tableau Map.

Idea is to clean the addresses in database and spit out correct addresses back to database
Data Flow 
Read the Database for addresses
Convert the addresses to single line list addresses for geocoding
Parse these addresses through GIS Website via Web Scrapper 
Return addresses along with their score
Get other attributes such as School District,  Police District, Sewage District etc.
Put Data back in Data Frame 
Write it back to Database
Repeat Step 1 for new/non-geocoded addresses 
