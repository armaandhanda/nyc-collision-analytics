-- KPIs --

-- Crashes by Area (Borough) per Time Period (Year)
-- NOTE: We can take Zip Code instead of Borough & Month/Quarter instead of Year
SELECT l.borough, d.year, COUNT(*) AS num_of_crashes
FROM crash_fact c
JOIN location_dim l ON c.location_key = l.location_key
JOIN date_dim d ON c.date_key = d.date_key
GROUP BY l.borough, d.year
ORDER BY d.year, l.borough;

-- Number of People Injured by Contributing Factor per Time Period (Year)
-- NOTE: We can take Month/Quarter instead of Year
SELECT c1.cont_factor AS contributing_factor, d.year,
SUM(c.num_persons_injured) AS number_of_people_injured
FROM crash_fact c
JOIN cont_factor_dim c1 ON c.cont_factor_key_1 = c1.cont_factor_key
JOIN date_dim d ON c.date_key = d.date_key
GROUP BY c1.cont_factor, d.year
ORDER BY d.year, number_of_people_injured DESC;

-- Number of People Killed by Contributing Factor per Time Period (Year)
-- NOTE: We can take Month/Quarter instead of Year
SELECT c1.cont_factor AS contributing_factor, d.year,
SUM(c.num_persons_killed) AS number_of_people_killed
FROM crash_fact c
JOIN cont_factor_dim c1 ON c.cont_factor_key_1 = c1.cont_factor_key
JOIN date_dim d ON c.date_key = d.date_key
GROUP BY c1.cont_factor, d.year
ORDER BY d.year, number_of_people_killed DESC;

-- Injuries per Combination of the types of Vehicles in Crash per Time Period (Year)
-- NOTE: We can take Deaths instead of Injuries & Month/Quarter instead of Year
SELECT 
CASE WHEN v1.veh_type < v2.veh_type THEN v1.veh_type ELSE v2.veh_type END AS vehicle_type_1, -- CASE statement to combine 'sedan & bus' and 'bus & sedan' type combinations
CASE WHEN v1.veh_type < v2.veh_type THEN v2.veh_type ELSE v1.veh_type END AS vehicle_type_2, 
d.year,
SUM(c.num_persons_injured) AS number_of_people_injured
FROM crash_fact c
JOIN veh_type_dim v1 ON c.veh_type_key_1 = v1.veh_type_key
JOIN veh_type_dim v2 ON c.veh_type_key_2 = v2.veh_type_key
JOIN date_dim d ON c.date_key = d.date_key
GROUP BY 
CASE WHEN v1.veh_type < v2.veh_type THEN v1.veh_type ELSE v2.veh_type END,
CASE WHEN v1.veh_type < v2.veh_type THEN v2.veh_type ELSE v1.veh_type END,
d.year
ORDER BY d.year, number_of_people_injured DESC;

-- Injury Rate (Injuries as a % of the number of crashes) by Area (Borough) per Time Period (Year)
-- NOTE: We can take Death Rate instead of Injury Rate, Zip Code instead of Borough & Month/Quarter instead of Year
SELECT l.borough, d.year, (SUM(c.num_persons_injured)/COUNT(*))*100 AS "INJURY RATE (%)"
FROM crash_fact c
JOIN location_dim l ON c.location_key = l.location_key
JOIN date_dim d ON c.date_key = d.date_key
GROUP BY l.borough, d.year
ORDER BY d.year, "INJURY RATE (%)" DESC;