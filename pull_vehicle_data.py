import argparse
import psycopg2
import csv
from sshtunnel import SSHTunnelForwarder
import time

# Local PostgreSQL Server connection parameters
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "auth"
DB_USER = "auth_admin"
DB_PASSWORD = "nimad_htua"

# Test PostgreSQL Server connection parameters
TEST_DB_HOST = "zeva-test-cluster.cluster-ro-czwgqn1npj54.ca-central-1.rds.amazonaws.com"
TEST_DB_USER = "zeva"
TEST_DB_PASSWORD = "jerrysocool"

# Staging PostgreSQL Server connection parameters
STAGING_DB_HOST = (
    "zeva-stage-cluster.cluster-ro-czwgqn1npj54.ca-central-1.rds.amazonaws.com"
)
STAGING_DB_USER = "zeva"
STAGING_DB_PASSWORD = "jerrysocool"

# Production PostgreSQL Server connection parameters
PROD_DB_HOST = "zeva-prod-cluster.cluster-ro-czwgqn1npj54.ca-central-1.rds.amazonaws.com"
PROD_DB_USER = "zeva_readonly"
PROD_DB_PASSWORD = "KD9`;uW4m}H-)wM._',CeE"

# Define the function to export the data
def extract_csv(csv_name, column_name, table_data):
    """This function is used to output the extracted csv file to the given table

    Args:
        csv_name (str): The csv file name that I am going to write the table to
        column_name (list of str): This contains all the columns that the csv file should contain
        table_data (data): The data returned by the fetch all
    """
    # This function helps to extract the data to the existing csv file
    # I would need to analyze whether it would be possible that to optimize the for row
    # Use pandas might be a better option
    with open(csv_name, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(column_name)
        csvwriter.writerows(table_data)
    csvfile.close()


# SQL script
PULL_CLIENT_COMPANY_RELATION_SQL = """
    SELECT 
        u.id AS userprofile_id, 
        u.first_name AS user_firstname,
        u.last_name AS user_lastname,
        u.country AS user_country,
        u.gender AS user_gender,
        u.postal_code as postal_code,
        c.id AS company_id,
        c.company_db AS company_db,
        c.data_type AS company_type,
        c.city AS company_city,
        c.country as company_country
    FROM 
        client_userprofile u
    JOIN 
        client_userprofile_companies uc ON u.id = uc.userprofile_id
    JOIN 
        client_company c ON uc.company_id = c.id;
"""

PULL_USERPROFILE_SQL = """
    SELECT
        *
    FROM
        client_userprofile;
"""

PULL_COMPANY_SQL = """
    SELECT
        *
    FROM
        client_company;
"""

PULL_CLIENT_PREFERENCE = """
    SELECT
        *
    FROM
        client_userconfig;
"""

PULL_VEHICLE_TRIP = """
    SELECT 
        * 
    FROM 
        vehicle_trip 
    LEFT JOIN vehicle_vehicle ON 
        vehicle_trip.vehicle_id = vehicle_vehicle.id 
    WHERE 
        start_timestamp between '2025-01-01' and '2025-09-30';
"""

PULL_VEHICLE_COMPANY = """
    SELECT 
        * 
    FROM 
        vehicle_vehicle
    LEFT JOIN client_company ON 
        vehicle_vehicle.company_id = client_company.id;
"""

PULL_VEHICLE_CHARGING = """
    SELECT 
	    *
    FROM 
        vehicle_chargesession
    LEFT JOIN vehicle_vehicle ON 
        vehicle_chargesession.vehicle_id = vehicle_vehicle.id 
    WHERE 
        start_timestamp between '2025-01-01' and '2025-09-30';
"""

PULL_VEHICLE_ANALYTICS = """
    SELECT 
        * 
    FROM 
        vehicle_vehicleanalytics
    LEFT JOIN vehicle_vehicle ON 
        vehicle_vehicleanalytics.vehicle_id = vehicle_vehicle.id 
    WHERE 
        timestamp between '2025-01-01' and '2025-09-30';
"""

PULL_VEHICLE_ALL = """
    SELECT 
        * 
    FROM 
        vehicle_vehicle;
"""

PULL_VEHICLE_INFOMATION_SUB = """
    SELECT
        id, vin, model, display_name
    FROM
        vehicle_vehicle
"""


PULL_HISTORICAL_STATE_SQL = """
    SELECT
        vehicle_vehiclehistoricalstate.id,
        timestamp,
        shift_state,
        latitude,
        longitude,
        odometer,
        speed_mph,
        heading,
        battery_level,
        battery_range_miles,
        is_charging,
        charge_amps,
        charger_voltage,
        charger_type,
        time_to_full_charge_min,
        charge_miles_added_ideal,
        charge_miles_added_rated,
        ideal_battery_range,
        est_battery_range,
        charge_energy_added,
        closest_road,
        grid_point,
        trip_id,
        vehicle_id,
        active_route_destination,
        active_route_latitude,
        active_route_longitude,
        active_route_miles_to_arrival,
        active_route_minutes_to_arrival,
        active_route_traffic_minutes_delay,
        bioweapon_mode,
        climate_keeper_mode,
        sentry_mode,
        valet_mode,
        utc_offset,
        cabin_overheat_protection,
        cop_activation_temperature,
        driver_temp_setting,
        vehicle_vehicle.v_id as vehicle_vid,
        vehicle_vehicle.vin as vehicle_vin,
        vehicle_vehicle.license_plate as license_plate,
        vehicle_vehicle.display_name as vehicle_name,
        vehicle_vehicle.model as vehicle_model,
        vehicle_vehicle.color as vehicle_color,
        vehicle_vehicle.firmware_version as vehicle_firmare_version,
        vehicle_vehicle.model_year as vehicle_model_year
    FROM vehicle_vehiclehistoricalstate
    LEFT JOIN vehicle_vehicle on
        (vehicle_vehiclehistoricalstate.vehicle_id = vehicle_vehicle.id)
    where timestamp >= '2025-07-01'
        order by vin asc, timestamp asc
"""

PULL_HISTORICAL_STATE_SQL_NEW = """
    WITH filtered_vehiclehistoricalstate AS (
    SELECT *
    FROM vehicle_vehiclehistoricalstate
    WHERE timestamp >= '2025-04-01'
)
SELECT
    vhs.id,
    vhs.timestamp,
    vhs.shift_state,
    vhs.latitude,
    vhs.longitude,
    vhs.odometer,
    vhs.speed_mph,
    vhs.heading,
    vhs.battery_level,
    vhs.battery_range_miles,
    vhs.is_charging,
    vhs.charge_amps,
    vhs.charger_voltage,
    vhs.charger_type,
    vhs.time_to_full_charge_min,
    vhs.charge_miles_added_ideal,
    vhs.charge_miles_added_rated,
    vhs.ideal_battery_range,
    vhs.est_battery_range,
    vhs.charge_energy_added,
    vhs.closest_road,
    vhs.grid_point,
    vhs.trip_id,
    vhs.vehicle_id,
    vhs.active_route_destination,
    vhs.active_route_latitude,
    vhs.active_route_longitude,
    vhs.active_route_miles_to_arrival,
    vhs.active_route_minutes_to_arrival,
    vhs.active_route_traffic_minutes_delay,
    vhs.bioweapon_mode,
    vhs.climate_keeper_mode,
    vhs.sentry_mode,
    vhs.valet_mode,
    vhs.utc_offset,
    vhs.cabin_overheat_protection,
    vhs.cop_activation_temperature,
    vhs.driver_temp_setting,
    vv.v_id AS vehicle_vid,
    vv.vin AS vehicle_vin,
    vv.license_plate AS license_plate,
    vv.display_name AS vehicle_name,
    vv.model AS vehicle_model,
    vv.color AS vehicle_color,
    vv.firmware_version AS vehicle_firmware_version,
    vv.model_year AS vehicle_model_year
FROM filtered_vehiclehistoricalstate vhs
LEFT JOIN vehicle_vehicle vv ON vhs.vehicle_id = vv.id
ORDER BY vv.vin ASC, vhs.timestamp;
"""


# output csv file
HISTORICAL_STATE_COLUMN = [
    "timestamp",
    "shift_state",
    "latitude",
    "longitude",
    "odometer",
    "speed_mph",
    "battery_level",
    "battery_range_miles",
    "is_charging",
    "charger_voltage",
    "charger_type",
    "trip_id",
    "vehicle_trip.start_timestamp as trip_start_timestamp",
    "vehicle_trip.end_timestamp as trip_end_timestamp",
    "vehicle_trip.starting_idle_timestamp as trip_starting_idle_timestamp",
    "vehicle_trip.starting_odometer as trip_starting_odometer",
    "vehicle_trip.end_odometer as trip_end_odometer",
    "vehicle_trip.starting_battery_level as trip_starting_battery_level",
    "vehicle_trip.end_battery_level as trip_end_battery_level",
    "vin",
    "license_plate",
    "display_name",
    "model",
    "color",
    "low_battery_alert_threshold",
    "speed_alert_threshold",
    "overcharge_alert_threshold",
    "user_color",
    "heading",
    "time_to_full_charge_min",
    "charge_amps",
    "charge_miles_added_ideal",
    "charge_miles_added_rated",
    "est_battery_range",
    "ideal_battery_range",
    "charge_energy_added",
    "closest_road",
    "active_route_destination",
    "active_route_latitude",
    "active_route_longitude",
    "active_route_miles_to_arrival",
    "active_route_minutes_to_arrival",
    "active_route_traffic_minutes_delay",
    "bioweapon_mode",
    "climate_keeper_mode",
    "sentry_mode",
    "valet_mode",
    "utc_offset",
    "cabin_overheat_protection",
    "cop_activation_temperature",
    "driver_temp_setting",
]

# Those three are best examples for the multiple data fetching
DRIVER_COMPANY_FILE_NAME = 'userprofile_company_full_table_sep.csv'
USERPROFILE_FILE_NAME = 'userprofile_full_sep.csv'
COMPANY_FILE_NAME = 'company_full_sep.csv'
CLIENT_PREFERENCE_NAME = 'user_preference/user_config.csv'
CLIENT_COMPANY_NAME = 'user_company/company.csv'
HISTORICAL_STATE_FILE_NAME = 'historicalstate/{db_name}.csv'
HISTORICAL_STATE_SEP_NOV = 'historicalstate_sep_nov/{db_name}.csv'
HISTORICAL_STATE_2025 = 'historicalstate_2025_04/{db_name}.csv'
HISTORICAL_TRIP_FILE_NAME = 'historicaltrip/{db_name}.csv'
HISTORICAL_CHARGING_FILE_NAME = 'historicalcharging/{db_name}.csv'
HISTORICAL_VEHICLEANALYTICS_FILE_NAME = 'historicalanalytics/{db_name}.csv'
VEHICLE_LIST_FILE_NAME = 'vehicles/{db_name}.csv'
PULL_VEHICLE_TRIP_FILE_NAME = 'vehicle_trip_data/{db_name}.csv'
PULL_CLIENT_DATA_FILE_NAME = 'vehicle_trip_data/{db_name}.csv'
VEHICLE_ANALYTICS_FILE_NAME = 'vehicle_vehicleanalytics/{db_name}.csv'
SUB_VEHICLE_COMPANY_FILE_NAME = 'vehicle_company/{db_name}.csv'



def pull_vehicle_data():
    # Connect to the database
    start_time = time.time()
    with psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT name, owner, password FROM client_database order by id",
            )
            db_info_list = cursor.fetchall()
            # db_info_list = [desc[0] for desc in cursor.description]
            # I can add the driver and company relationship under this section
            # Fetch the existing client and company relationship
            # NOTE: This is one-time fetch. Only need to fetch for one time
            # cursor.execute(PULL_COMPANY_SQL)
            # driver_company_relationship = cursor.fetchall()
            # client_company_column_names = [desc[0] for desc in cursor.description]
            # extract_csv(CLIENT_COMPANY_NAME, client_company_column_names, driver_company_relationship)
            # # Fetch the existing userprofile
            # cursor.execute(PULL_USERPROFILE_SQL)
            # userprofile = cursor.fetchall()
            # userprofile_column_names = [desc[0] for desc in cursor.description]
            # extract_csv(USERPROFILE_FILE_NAME, userprofile_column_names, userprofile)
            # # Fetch the existing company list
            # cursor.execute(PULL_COMPANY_SQL)
            # company = cursor.fetchall()
            # company_column_names = [desc[0] for desc in cursor.description]
            # extract_csv(COMPANY_FILE_NAME, company_column_names, company)
            # # Fetch the relationship between the client and company
            # cursor.execute(PULL_CLIENT_COMPANY_RELATION_SQL)
            # userprofile_company = cursor.fetchall()
            # client_company_columns = [desc[0] for desc in cursor.description]
            # extract_csv(DRIVER_COMPANY_FILE_NAME, client_company_columns, userprofile_company)
            
    print('---------%s seconds to do the auth checking -----------' % (time.time() - start_time))

    # TEMP_DB_HOST = DB_HOST
    # TEMP_DB_PORT = DB_PORT
    # TEMP_DB_NAME = DB_NAME
    # TEMP_DB_USER = DB_USER
    # TEMP_DB_PASSWORD = DB_PASSWORD
    # temp_connection = None
    # temp_cursor = None
    # for db_info in db_info_list:
    #     loop_start_time = time.time()
    #     print(db_info)
    #     if db_info[0] == 'auth':
    #         continue
    #     TEMP_DB_NAME = db_info[0]
    #     TEMP_DB_USER = db_info[1]
    #     TEMP_DB_PASSWORD = db_info[2]
    #     with psycopg2.connect(
    #         host=TEMP_DB_HOST, port=TEMP_DB_PORT, database=TEMP_DB_NAME, user=TEMP_DB_USER, password=TEMP_DB_PASSWORD
    #     ) as temp_connection:
    #         with temp_connection.cursor() as temp_cursor:
    #             temp_cursor.execute(PULL_VEHICLE_COMPANY)
    #             result = temp_cursor.fetchall()
    #             column_names = [desc[0] for desc in temp_cursor.description]
    #             extract_csv(SUB_VEHICLE_COMPANY_FILE_NAME.format(db_name = TEMP_DB_NAME), column_names, result)
    #             print('---------%s seconds to perform one time data fetching -----------' % (time.time() - loop_start_time))
    #     if temp_cursor:
    #         temp_cursor.close()
    #     if temp_connection:
    #         temp_connection.close()
    # print('---------%s seconds to perform all data fetching -----------' % (time.time() - start_time))



if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Purge users from the database.")
    parser.add_argument(
        "-T", "--test", action="store_true", help="Run the script in test environment"
    )
    parser.add_argument(
        "-S",
        "--staging",
        action="store_true",
        help="Run the script in staging environment",
    )
    parser.add_argument(
        "-P",
        "--prod",
        action="store_true",
        help="Run the script in production environment",
    )
    args = parser.parse_args()

    # Set database connection parameters for test environment, if specified
    if args.test:
        DB_HOST = TEST_DB_HOST
        DB_USER = TEST_DB_USER
        DB_PASSWORD = TEST_DB_PASSWORD

    # Set database connection parameters for staging environment, if specified
    if args.staging:
        DB_HOST = STAGING_DB_HOST
        DB_USER = STAGING_DB_USER
        DB_PASSWORD = STAGING_DB_PASSWORD

    # Set database connection parameters for production environment, if specified
    if args.prod:
        DB_HOST = PROD_DB_HOST
        DB_USER = PROD_DB_USER
        DB_PASSWORD = PROD_DB_PASSWORD

        # Prompt the user to confirm the execution in production environment
        print("This script will run in production environment.")
        prompt = input("Type 'PRODUCTION' to confirm: ")
        if prompt != "PRODUCTION":
            print("Execution aborted.")
            exit()

        try:
            server = SSHTunnelForwarder(
                ("3.97.126.230", 22),
                ssh_username="ubuntu",
                ssh_private_key="/Users/jialianglin/Desktop/Credential/zeva-prod-db.pem",
                remote_bind_address=(DB_HOST, DB_PORT),
            )
            server.start()
            DB_PORT = server.local_bind_port
            DB_HOST = "localhost"

            # Purge the databases
            pull_vehicle_data()
        except Exception as e:
            print("SSH error: ", e)
        finally:
            if server:
                server.stop()
    else:
        pull_vehicle_data()
