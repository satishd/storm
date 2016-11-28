CREATE TABLE weather.temperature (
    weatherstation_id   TEXT,
    event_time          BIGINT,
    temperature         BIGINT,
    PRIMARY KEY(weatherstation_id, event_time)
)