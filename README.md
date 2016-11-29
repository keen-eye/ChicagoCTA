# ChicagoCTA

Code developed for Chicago Transit Authority project, the capstone work for a fellowship with the Data Incubator.

build_anchor_dict.py queries concise descriptions of bus routes and schedules to pull out major stops to use as keys on railroad graphs that are to be built down the line. It also uses logic to determine route variations and relative positions of stops that may vary depending on route direction.

write_database.py queries the CTA API every minute and records all active bus locations to be stored in a SQL database. Relevant information is extracted from the returned XML to be saved.

pattern_database.py queries the CTA API to get route shape descriptions for all new patterns added in the previous 30 minutes of calls.

make_RRplots.py automatedly makes railroad graphs using the databased data for specific input days, making full plots as a weekday, Saturday and Sunday schedules.

rr_graph_realtime.py is intended to construct railroad graphs on demand from a socket connection from a front-end visualization website
