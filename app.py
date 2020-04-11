import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from flask import Flask, jsonify
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
import datetime as dt
import pandas as pd

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
measurement = Base.classes.measurement
allstations = Base.classes.station


# Flask Setup
app = Flask(__name__)

@app.route("/")
def welcome():
    return (
        f"Welcome to Hawaii Weather Station<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def Precipitation_data():

    """Convert the query results to a dictionary using date as the key and prcp as the value"""

    # Create our session (link) from Python to the DB
    session = Session(engine)

    latest_date = session.query(measurement.date).order_by(measurement.date.desc()).first()
    latest_date = pd.to_datetime(latest_date)
    start_date = (latest_date - dt.timedelta(days=365)).to_pydatetime()[0]

    results = session.query(measurement.date, measurement.prcp).filter(measurement.date > start_date).all()
    session.close()

    # Create a dictionary from the row data and append to a list
    last_year_values = []
    for date, prcp in results:
        temp_dict = {}
        temp_dict ["date"] = date
        temp_dict ["prcp"] = prcp
        last_year_values.append(temp_dict)

    return jsonify(last_year_values)


@app.route("/api/v1.0/stations")
def get_stations():

    """Return a JSON list of stations from the dataset."""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    results = session.query(allstations.station, allstations.name, allstations.latitude, allstations.longitude, allstations.elevation).all()
    session.close()

    # Create a dictionary from the row data and append to a list
    stations = []
    for station, name, latitude, longitude, elevation in results:
        stn_dict = {}
        stn_dict ["station"] = station
        stn_dict ["name"] = name
        stn_dict["latitude"] = latitude
        stn_dict["longitude"] = longitude
        stn_dict["elevation"] = elevation
        stations.append(stn_dict)
    return jsonify(stations)


@app.route("/api/v1.0/tobs")
def get_temperatures():

    """Query the dates and temperature observations of the most active station for the last year of data."""
    # Create our session (link) from Python to the DB
    session = Session(engine)

    latest_date = session.query(measurement.date).order_by(measurement.date.desc()).first()
    latest_date = pd.to_datetime(latest_date)
    start_date = (latest_date - dt.timedelta(days=365)).to_pydatetime()[0]

    # most active station in the last year of data
    station_id = session.query(measurement.station, func.count(measurement.station)).\
    filter(measurement.date > start_date).\
    group_by(measurement.station).\
    order_by(func.count(measurement.station).desc()).first()[0]


    # Last year measurements for the most active station
    results = session.query(measurement.date, measurement.tobs).\
    filter(measurement.station == station_id).\
    filter(measurement.date > start_date).all()
    session.close()

    # Create a dictionary from the row data and append to a list
    temps = []
    for date, tobs in results:
        temp_dict = {}
        temp_dict ["date"] = date
        temp_dict["tobs"] = tobs
        temps.append(temp_dict)
    return jsonify(temps)


@app.route("/api/v1.0/<startdate>")
def get_stats_start_date(startdate):
    """Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range."""

    try:
        valid_date = dt.datetime.strptime(startdate, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f'{startdate} is not valid date in the format YYYY-MM-DD')

    session = Session(engine)

    results = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
    filter(measurement.date >= valid_date).all()

    session.close()

    # Create a dictionary from the row data and append to a list
    temps = []
    temp_dict = {}
    temp_dict ["min"] = results[0][0]
    temp_dict["avg"] = results[0][1]
    temp_dict["max"] = results[0][2]
    temps.append(temp_dict)
    return jsonify(temps)

@app.route("/api/v1.0/<startdate>/<enddate>")
def get_stats_start_end_date(startdate, enddate):
    """Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range."""

    try:
        valid_start_date = dt.datetime.strptime(startdate, "%Y-%m-%d").date()

    except ValueError:
        raise ValueError(f'{startdate} is not valid date in the format YYYY-MM-DD')

    try:
        valid_end_date = dt.datetime.strptime(enddate, "%Y-%m-%d").date()

    except ValueError:
        raise ValueError(f'{enddate} is not valid date in the format YYYY-MM-DD')


    session = Session(engine)

    results = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
    filter(measurement.date >= valid_start_date).filter(measurement.date <= valid_end_date).all()

    session.close()

    # Create a dictionary from the row data and append to a list
    temps = []
    temp_dict = {}
    temp_dict ["min"] = results[0][0]
    temp_dict["avg"] = results[0][1]
    temp_dict["max"] = results[0][2]
    temps.append(temp_dict)
    return jsonify(temps)

if __name__ == "__main__":
    app.run(debug=True)

