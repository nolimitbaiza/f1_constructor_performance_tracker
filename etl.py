import fastf1 as ff1
import pandas as pd
from sqlalchemy import create_engine
import time

# --- Database Connection ---
DB_HOST = 'your_db_host'
DB_NAME = 'f1_data'
DB_USER = 'your_db_user'
DB_PASS = 'your_db_password'
DB_PORT = '5432'

# Creates the database connection string
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Creates the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# --- FastF1 Cache ---
ff1.Cache.enable_cache('fastf1_cache')


def get_race_results(year):
    """
    Fetches all race results for a given year and returns a clean DataFrame.
    """
    print(f"Fetching data for {year} season...")
    # Creates an empty list to store results for each race
    all_results_df = []

    # Gets the schedule for the year
    schedule = ff1.get_event_schedule(year)
    races = schedule[schedule['RoundNumber'] > 0]

    # Loops through each race in the schedule
    for index, race in races.iterrows():
        try:
            # Loads the session (Race)
            session = ff1.get_session(year, race['RoundNumber'], 'R')
            session.load()  # Load data

            # Gets results DataFrame
            results = session.results

            # Adds metadata from the schedule
            results['Season'] = year
            results['RoundNumber'] = race['RoundNumber']
            results['RaceName'] = race['EventName']

            # Selects and rename columns for clarity in our DB
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

            # Renames columns to be more SQL-friendly (e.g., lowercase)
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
            print(f"  > !! Error loading session for {year} {race['EventName']}: {e}")

    # Combines all individual race DataFrames into one
    if not all_results_df:
        return pd.DataFrame()

    final_df = pd.concat(all_results_df, ignore_index=True)
    return final_df


def main():
    # --- Defines Scope ---
    CURRENT_YEAR = pd.to_datetime('today').year
    YEARS_TO_LOAD = range(2018, CURRENT_YEAR + 1)

    all_seasons_df = []

    for year in YEARS_TO_LOAD:
        start_time = time.time()
        season_results = get_race_results(year)
        all_seasons_df.append(season_results)
        end_time = time.time()
        print(f"Season {year} processed in {end_time - start_time:.2f} seconds\n")

    # Combines all seasons into one giant DataFrame
    final_all_data = pd.concat(all_seasons_df, ignore_index=True)

    print("\nStarting to load all data into the database...")
    try:
        # Loads the DataFrame into a SQL table named 'race_results'
        final_all_data.to_sql(
            'race_results',
            engine,
            if_exists='replace',
            index=False,
            method='multi'
        )
        print("✅ Data successfully loaded into 'race_results' table.")
    except Exception as e:
        print(f"❌ Error loading data to database: {e}")


if __name__ == "__main__":
    main()
