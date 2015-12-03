CREATE TABLE temperature (
    weather_station_id   TEXT,
    weather_station_name   TEXT,
    event_time          TIMEUUID,
    temperature         TEXT,
    PRIMARY KEY(weather_station_id, event_time)
);

CREATE TABLE station (
    id TEXT,
    name TEXT,
    PRIMARY KEY(id)
);