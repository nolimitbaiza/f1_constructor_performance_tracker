CREATE SCHEMA IF NOT EXISTS f1;
CREATE TABLE IF NOT EXISTS f1.constructors (id integer PRIMARY KEY, ref text UNIQUE NOT NULL, name text NOT NULL, nationality text NOT NULL);
CREATE TABLE IF NOT EXISTS f1.seasons (year integer PRIMARY KEY, races integer NOT NULL, scheduled integer NOT NULL, complete boolean NOT NULL);
CREATE TABLE IF NOT EXISTS f1.races (id integer PRIMARY KEY, season integer REFERENCES f1.seasons(year), round integer NOT NULL, name text NOT NULL, date text NOT NULL, UNIQUE(season, round));
CREATE TABLE IF NOT EXISTS f1.results (
  "raceId" integer REFERENCES f1.races(id), "constructorId" integer REFERENCES f1.constructors(id),
  wins integer NOT NULL CHECK(wins >= 0), podiums integer NOT NULL CHECK(podiums >= wins), entries integer NOT NULL CHECK(entries >= podiums),
  classified integer NOT NULL CHECK(classified >= 0 AND classified <= entries), "finishSum" integer NOT NULL,
  poles integer NOT NULL, "racePoints" double precision NOT NULL, "sprintPoints" double precision NOT NULL,
  "championshipPoints" double precision, rank integer, PRIMARY KEY("raceId", "constructorId")
);
CREATE TABLE IF NOT EXISTS f1.standings ("raceId" integer REFERENCES f1.races(id), "constructorId" integer REFERENCES f1.constructors(id), points double precision NOT NULL, rank integer NOT NULL, excluded boolean NOT NULL, PRIMARY KEY("raceId", "constructorId"));
CREATE INDEX IF NOT EXISTS results_constructor_idx ON f1.results("constructorId");
CREATE TABLE IF NOT EXISTS f1.metadata (id integer PRIMARY KEY CHECK(id = 1), data jsonb NOT NULL);
