db = db.getSiblingDB('flightsdb');
db.createCollection('flights');
db.flights.createIndex({ fl_date:1, op_unique_carrier:1, op_carrier_fl_num:1, origin:1, dest:1 }, { name:"uniq_route_day_flight", unique:false });
db.flights.createIndex({ op_unique_carrier:1, fl_date:1 }, { name:"carrier_day" });
db.flights.createIndex({ origin:1, fl_date:1 }, { name:"origin_day" });
db.flights.createIndex({ origin:1, dest:1, fl_date:1 }, { name:"route_day" });
db.flights.createIndex({ arr_delay:1 }, { name:"arr_delay" });
