#!/usr/bin/env python
from octopus_energy_api import oe_api
import click
from influxdb import InfluxDBClient
import os
from dateutil import parser
from datetime import date, datetime, timedelta
import time

def store_series(connection, series, metrics, conversion_factor=None):

    def fields_for_measurement(measurement, conversion_factor=None):
        raw_consumption = measurement['consumption']
        if conversion_factor:
            consumption = raw_consumption * conversion_factor
        else:
            consumption = raw_consumption
        return {
            'consumption': consumption,
            'raw_consumption': raw_consumption
        }
        

    def tags_for_measurement(measurement):
        return {'time_of_day': datetime.now().strftime('%H:%M') }

    measurements = [
        {
            'measurement': series,
            'tags': tags_for_measurement(measurement),
            'time': measurement['interval_end'],
            'fields': fields_for_measurement(measurement, conversion_factor),
        }
        for measurement in metrics
    ]
    connection.write_points(measurements)

@click.command()
def monitor():
    influx = InfluxDBClient(
        host=os.getenv("INFLUX_DB_HOST") if os.getenv("INFLUX_DB_HOST") else "localhost",
        port=os.getenv("INFLUX_DB_PORT") if os.getenv("INFLUX_DB_PORT") else 8086,
        username=os.getenv("INFLUX_DB_USER") if os.getenv("INFLUX_DB_USER") else "",
        password=os.getenv("INFLUX_DB_PASSWORD") if os.getenv("INFLUX_DB_PASSWORD") else "",
        database=os.getenv("INFLUX_DB_NAME") if os.getenv("INFLUX_DB_NAME") else "energy",
    )

    api_key = os.getenv('OCTOPUS_API_KEY')
    if not api_key:
        raise click.ClickException('No Octopus API key set.')
    
    account_no = os.getenv('OCTOPUS_ACCOUNT_NO')
    if not account_no:
        raise click.ClickException('No Octopus Account number set.')

    e_mpan = os.getenv('ELECTRICITY_MPAN')
    if not e_mpan:
        raise click.ClickException('No mpan set for electricity meter.')
    
    e_serial =  os.getenv('ELECTRICITY_SERIAL_NO')
    if not e_serial:
        raise click.ClickException('No serial number set for electricity meter.')

    g_mpan = os.getenv('GS_MPAN')
    if not g_mpan:
        raise click.ClickException('No mpan set for gas meter.')

    g_serial = os.getenv('GAS_SERIAL_NO')
    if not g_serial:
        raise click.ClickException('No serial number set for gas meter.')

    volume_correction_factor = os.getenv('VOLUME_CORRECTION_FACTOR')
    if not volume_correction_factor:
        raise click.ClickException('No volume correction factor set.')
    volume_correction_factor = float(volume_correction_factor)

    # if for some reason this script is still running after a year, we'll stop after 365 days
    for i in range(0,365):
        from_date = date.today() - timedelta(days=1)
        to_date = date.today()

        # Load electricity consumption
        e_energy_api = oe_api(account_no, api_key, mpan=e_mpan, serial_number=e_serial)
        e_consumption = e_energy_api.consumption(from_date, to_date)
        click.echo(f"Loaded elctricity data between {from_date} to {to_date}. {len(e_consumption)} results found.")
        store_series(influx, 'electricity', e_consumption, conversion_factor=None)

        # Load gas consumption
        g_energy_api = oe_api(account_no, api_key, mpan=g_mpan, serial_number=g_serial)
        g_consumption = g_energy_api.consumption(from_date, to_date)
        click.echo(f"Loaded gas data between {from_date} to {to_date}. {len(g_consumption)} results found.")
        store_series(influx, 'gas', g_consumption, conversion_factor=volume_correction_factor)

        # Sleep until 2AM
        today = datetime.today()
        future = datetime(today.year,today.month,today.day,2,0)
        if today.hour >= 2:
            future += timedelta(days=1)
        click.echo(f"Next data pull will be on {future}.")
        time.sleep((future - today).total_seconds())

if __name__ == '__main__':
    monitor()