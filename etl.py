import fastf1 as ff1
import pandas as pd
from sqlalchemy import create_engine
import time

# --- Database Connection ---
# Replace with your own database credentials
DB_HOST = 'your_db_host'
DB_NAME = 'f1_data'
DB_USER = 'your_db_user'
DB_PASS = 'your_db_password'
DB_PORT = '5432'  # Default PostgreSQL port

# Create the database connection string
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# --- FastF1 Cache ---
# Make sure cache is enabled
ff1.Cache.enable_cache('fastf1_cache')


def get_race_results(year):
    """
    Fetches all race results for a given year and returns a clean DataFrame.
    """
    print(f"Fetching data for {year} season...")
    # Create an empty list to store results for each race
    all_results_df = []

    # Get the schedule for the year
    schedule = ff1.get_event_schedule(year)

    # We only want to get data for races (RoundNumber > 0)
    races = schedule[schedule['RoundNumber'] > 0]

    # Loop through each race in the schedule
    for index, race in races.iterrows():
        try:
            # Load the session (Race)
            session = ff1.get_session(year, race['RoundNumber'], 'R')
            session.load()  # Load data

            # Get results DataFrame
            results = session.results

            # --- This is the TRANSFORM step ---
            # Add metadata from the schedule
            results['Season'] = year
            results['RoundNumber'] = race['RoundNumber']
            results['RaceName'] = race['EventName']

            # Select and rename columns for clarity in our DB
            transformed_results = results[[
                'Season',
                'RoundNumber',
                'RaceName',
                'BroadcastName',
                'ConstructorName',
                'GridPosition',
                'Position',
                'Points',
                'Status'
            ]].copy()

            # Rename columns to be more SQL-friendly (e.g., lowercase)
            transformed_results.rename(columns={
                'BroadcastName': 'driver_name',
                'ConstructorName': 'constructor_name',
                'GridPosition': 'grid_position',
                'Position': 'position',
                'Points': 'points',
                'Status': 'status'
            }, inplace=True)

            all_results_df.append(transformed_results)
            print(f"  > Fetched data for {year} {race['EventName']}")

        except Exception as e:
            # If a session fails to load (e.g., race cancelled), skip it
            print(f"  > !! Error loading session for {year} {race['EventName']}: {e}")

    # Combine all individual race DataFrames into one
    if not all_results_df:
        return pd.DataFrame()  # Return empty if no data

    final_df = pd.concat(all_results_df, ignore_index=True)
    return final_df


def main():
    # --- Define Scope ---
    # Let's get data for the last 5 full seasons + the current season
    # Note: FastF1 data is available from 2018 onwards
    CURRENT_YEAR = pd.to_datetime('today').year
    YEARS_TO_LOAD = range(2018, CURRENT_YEAR + 1)

    all_seasons_df = []

    for year in YEARS_TO_LOAD:
        start_time = time.time()
        season_results = get_race_results(year)
        all_seasons_df.append(season_results)
        end_time = time.time()
        print(f"Season {year} processed in {end_time - start_time:.2f} seconds\n")

    # Combine all seasons into one giant DataFrame
    final_all_data = pd.concat(all_seasons_df, ignore_index=True)

    # --- This is the LOAD step ---
    print("\nStarting to load all data into the database...")
    try:
        # Load the DataFrame into a SQL table named 'race_results'
        # 'if_exists='replace'' will drop the table and recreate it.
        # This is simple and ensures data is fresh.
        # (For a real production system, you'd use 'append' and check for duplicates)
        final_all_data.to_sql(
            'race_results',
            engine,
            if_exists='replace',
            index=False,
            method='multi'  # Efficiently inserts data in chunks
        )
        print("✅ Data successfully loaded into 'race_results' table.")
    except Exception as e:
        print(f"❌ Error loading data to database: {e}")


# Run the main function when the script is executed
if __name__ == "__main__":
    main()
