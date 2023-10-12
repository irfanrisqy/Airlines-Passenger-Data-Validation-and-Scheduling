'''
=================================================
Milestone 3

Nama  : Irfan Risqy
Batch : FTDS-001-BSD

Program ini dibuat untuk pembuatan database baru dan import data dari dataset kedalam PostgreSQL.
=================================================
'''

-- Create Table
CREATE TABLE table_m3 (
	"id" INT PRIMARY KEY, 
	"Gender" VARCHAR(20), 
	"Customer Type" VARCHAR(20), 
	"Age" INT, 
	"Type of Travel" VARCHAR(20), 
	"Class" VARCHAR(20),
	"Flight Distance" INT, 
	"Inflight wifi service" INT,
	"Departure/Arrival time convenient" INT, 
	"Ease of Online booking" INT,
	"Gate location" INT, 
	"Food and drink" INT, 
	"Online boarding" INT, 
	"Seat comfort" INT,
	"Inflight entertainment" INT, 
	"On-board service" INT, 
	"Leg room service" INT,
	"Baggage handling" INT, 
	"Checkin service" INT, 
	"Inflight service" INT,
	"Cleanliness" INT, 
	"Departure Delay in Minutes" FLOAT, 
	"Arrival Delay in Minutes" FLOAT,
	"satisfaction" VARCHAR(20)
	);

-- Copy Data from Dataset
COPY table_m3(
	"id", 
	"Gender", 
	"Customer Type", 
	"Age", 
	"Type of Travel", 
	"Class",
	"Flight Distance", 
	"Inflight wifi service",
	"Departure/Arrival time convenient", 
	"Ease of Online booking",
	"Gate location", 
	"Food and drink", 
	"Online boarding", 
	"Seat comfort",
	"Inflight entertainment", 
	"On-board service", 
	"Leg room service",
	"Baggage handling", 
	"Checkin service", 
	"Inflight service",
	"Cleanliness", 
	"Departure Delay in Minutes", 
	"Arrival Delay in Minutes",
	"satisfaction"
) 
FROM 'C:/Users/irfan/OneDrive/Documents/Hacktiv8/phase2/p2-ftds001-bsd-m3-irfanrisqy/P2M3_irfan_risqy_data_raw.csv' 
DELIMITER ',' CSV HEADER;

-- Check Table
SELECT * FROM table_m3