# untuk membuat database
CREATE DATABASE raw_data;

#untuk membuat table sesuai kolom
CREATE TABLE table_gc7 (
  car_name VARCHAR(100),
  brand VARCHAR(100),
  year INTEGER,
  mileage NUMERIC(5),
  location VARCHAR(100),
  transmission VARCHAR(100),
  plate_type VARCHAR(100),
  rear_camera INTEGER,
  sun_roof INTEGER,
  auto_retract_mirror INTEGER,
  electric_parking_brake INTEGER,
  map_navigator INTEGER,
  vehicle_stability_control INTEGER,
  keyless_push_start INTEGER,
  sports_mode INTEGER,
  camera_view INTEGER,
  power_sliding_door INTEGER,
  auto_cruise_control INTEGER,
  price INTEGER,
  instalment INTEGER
)

# untuk membaca csv
COPY table_gc7 (
  car_name, 
  brand, 
  year,
  mileage,
  location,
  transmission,
  plate_type,
  rear_camera,
  sun_roof,
  auto_retract_mirror,
  electric_parking_brake,
  map_navigator,
  vehicle_stability_control,
  keyless_push_start,
  sports_mode,
  camera_view,
  power_sliding_door,
  auto_cruise_control,
  price,
  instalment
)
FROM 'C:\Users\Zaky\github-classroom\FTDS-assignment-bay\p2-ftds008-hck-g7-zakyramdhani\P2G7_zaky_ramdhani_data_raw.csv'
DELIMITER ','
CSV HEADER;

#untuk menampilkan seluruh table
SELECT * FROM table_gc7;