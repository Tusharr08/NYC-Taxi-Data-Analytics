
  
    

        create or replace transient table nyc_taxi_db.conformed.dim_location
         as
        (

SELECT 
    LocationID AS location_id,
    Borough AS borough,
    Zone AS zone
FROM nyc_taxi_db.seeds.taxi_zone_lookup
        );
      
  