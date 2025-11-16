import fastf1 as ff1
import pandas as pd
from sqlalchemy import create_engine
import time
import os
from dotenv import load_dotenv

load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

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
    all_results_df = []

    schedule = ff1.get_event_schedule(year)
    races = schedule[schedule['RoundNumber'] > 0]

    results = 0

    for index, race in races.iterrows():
        try:
            session = ff1.get_session(year, race['RoundNumber'], 'R')
            session.load()

            # Gets results DataFrame
            results = session.results

            # --- 💡 YOUR FIX IS HERE 💡 ---
            # Handles data inconsistency where 'ConstructorName' is 'TeamName' in older seasons
            if 'ConstructorName' not in results.columns and 'TeamName' in results.columns:
                results.rename(columns={'TeamName': 'ConstructorName'}, inplace=True)
            # --- END OF FIX ---

            # Adds metadata from the schedule
            results['Season'] = year
            results['RoundNumber'] = race['RoundNumber']
            results['RaceName'] = race['EventName']

            # Selects and rename columns
            # We add a check to make sure all columns exist before trying to select them
            required_columns = [
                'Season', 'RoundNumber', 'RaceName', 'BroadcastName',
                'ConstructorName', 'GridPosition', 'Position', 'Points', 'Status'
            ]

            # Find which of our required columns are actually in the results
            available_columns = [col for col in required_columns if col in results.columns]

            # Select only the columns that are available
            transformed_results = results[available_columns].copy()

            # Renames columns
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
            # Added a more specific check. If results are empty, skip.
            if 'results' in locals() and results.empty:
                print(f"  > !! No results data for {year} {race['EventName']}. Skipping.")
            else:
                print(f"  > !! Error loading session for {year} {race['EventName']}: {e}")

    if not all_results_df:
        return pd.DataFrame()

    # pd.concat can fail if columns are in a different order. This standardizes them.
    final_df = pd.concat(all_results_df, ignore_index=True, sort=False)
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
