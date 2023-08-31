#!/usr/bin/env python
import json
from dataclasses import dataclass
from urllib import parse

import maya
import requests
import yaml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


@dataclass
class InfluxDB:
    version: int
    url: str
    token: str
    bucket: str
    org: str


@dataclass
class Octopus:
    api_key: str


@dataclass
class Electricity:
    mpan: int
    serial_number: str
    unit_rate_high: float
    unit_rate_low: float
    unit_rate_low_start: str
    unit_rate_low_end: str
    unit_rate_time_zone: str
    standing_charge_url: str
    rate_url: str

    @property
    def url(self):
        return (
            "https://api.octopus.energy/v1/electricity-meter-points/"
            f"{self.mpan}/meters/{self.serial_number}/consumption/"
        )


@dataclass
class Gas:
    mpan: int
    serial_number: str
    standing_charge: float
    unit_rate: float
    meter_type: int
    volume_correction_factor: float
    calorific_value: float

    @property
    def url(self):
        return (
            "https://api.octopus.energy/v1/gas-meter-points/"
            f"{self.mpan}/meters/{self.serial_number}/consumption/"
        )


@dataclass
class Config:
    influxdb: InfluxDB
    octopus: Octopus
    electricity: Electricity
    gas: Gas

    def __post_init__(self):
        self.influxdb = InfluxDB(**self.influxdb)
        self.octopus = Octopus(**self.octopus)
        self.electricity = Electricity(**self.electricity)
        self.gas = Gas(**self.gas)

    @staticmethod
    def from_yaml(path):
        data = yaml.load(open(path), Loader=yaml.SafeLoader)
        return Config(**data)


def extract(api_key, series, from_date, to_date):
    def extract_paginated(api_key, url, from_date, to_date, page=None):
        args = {
            "period_from": from_date,
            "period_to": to_date,
        }
        if page:
            args["page"] = page
        response = requests.get(url, params=args, auth=(api_key, ""))
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        if data["next"]:
            url_query = parse.urlparse(data["next"]).query
            next_page = parse.parse_qs(url_query)["page"][0]
            results += extract_paginated(api_key, url, from_date, to_date, next_page)
        return results

    consumption = extract_paginated(api_key, series.url, from_date, to_date)
    standing_charge = (
        requests.get(series.standing_charge_url, auth=(api_key, ""))
        .json()
        .get("results", [])[0].get("value_inc_vat", 0)
        if hasattr(series, "standing_charge_url")
        else series.standing_charge
    )
    return consumption, standing_charge


def transform(series, metrics, rate_data):
    agile_data = rate_data.get("agile_unit_rates", [])
    agile_rates = {point["valid_to"]: point["value_inc_vat"] for point in agile_data}

    def active_rate_field(measurement):
        if series == "gas":
            return "unit_rate"
        elif not rate_data["unit_rate_low_zone"]:  # no low rate
            return "unit_rate_high"

        low_start_str = rate_data["unit_rate_low_start"]
        low_end_str = rate_data["unit_rate_low_end"]
        low_zone = rate_data["unit_rate_low_zone"]

        measurement_at = maya.parse(measurement["interval_start"])

        low_start = maya.when(
            measurement_at.datetime(to_timezone=low_zone).strftime(
                f"%Y-%m-%dT{low_start_str}"
            ),
            timezone=low_zone,
        )
        low_end = maya.when(
            measurement_at.datetime(to_timezone=low_zone).strftime(
                f"%Y-%m-%dT{low_end_str}"
            ),
            timezone=low_zone,
        )
        low_period = maya.MayaInterval(low_start, low_end)

        return "unit_rate_low" if measurement_at in low_period else "unit_rate_high"

    def fields_for_measurement(measurement):
        consumption = measurement["consumption"]
        conversion_factor = rate_data.get("conversion_factor", None)
        if conversion_factor:
            consumption *= conversion_factor
        rate = active_rate_field(measurement)
        rate_cost = rate_data[rate]
        cost = consumption * rate_cost
        standing_charge = rate_data["standing_charge"] / 48  # 30 minute reads
        fields = {
            "consumption": consumption,
            "cost": cost,
            "total_cost": cost + standing_charge,
        }
        if agile_data:
            agile_standing_charge = rate_data["agile_standing_charge"] / 48
            agile_unit_rate = agile_rates.get(
                maya.parse(measurement["interval_end"]).iso8601(),
                rate_data[rate],  # cludge, use Go rate during DST changeover
            )
            agile_cost = agile_unit_rate * consumption
            fields.update(
                {
                    "agile_rate": agile_unit_rate,
                    "agile_cost": agile_cost,
                    "agile_total_cost": agile_cost + agile_standing_charge,
                }
            )
        return fields

    def tags_for_measurement(measurement):
        period = maya.parse(measurement["interval_end"])
        time = period.datetime().strftime("%H:%M")
        return {
            "active_rate": active_rate_field(measurement),
            "time_of_day": time,
        }

    measurements = [
        {
            "measurement": series,
            "tags": tags_for_measurement(measurement),
            "time": measurement["interval_end"],
            "fields": fields_for_measurement(measurement),
        }
        for measurement in metrics
    ]
    return measurements


def load(connection, config, measurements, series):
    # connection.write(config.influxdb.bucket, config.influxdb.org, measurements)
    json.dump(measurements, open(f"{series}.json", "w"))


def main(
    config_file="octograph.ini",
    from_date="1 week ago midnight",
    to_date="today midnight",
):
    config = Config.from_yaml("octograph.yaml")

    influx = InfluxDBClient(
        url=config.influxdb.url,
        token=config.influxdb.token,
        org=config.influxdb.org,
    )
    connection = influx.write_api(write_options=SYNCHRONOUS)

    rate_data = {
        "electricity": {
            "unit_rate_high": config.electricity.unit_rate_high,
            "unit_rate_low": config.electricity.unit_rate_low,
            "unit_rate_low_start": config.electricity.unit_rate_low_start,
            "unit_rate_low_end": config.electricity.unit_rate_low_end,
            "unit_rate_low_zone": config.electricity.unit_rate_time_zone,
        },
        "gas": {
            "unit_rate": config.gas.unit_rate,
            "conversion_factor": config.gas.volume_correction_factor
            * config.gas.calorific_value
            / 3.6
            if config.gas.meter_type > 1
            else None,
        },
    }

    from_iso = maya.when(
        from_date, timezone=config.electricity.unit_rate_time_zone
    ).iso8601()
    to_iso = maya.when(
        to_date, timezone=config.electricity.unit_rate_time_zone
    ).iso8601()
    electricity, rate_data["electricity"]["standing_charge"] = extract(
        config.octopus.api_key, config.electricity, from_iso, to_iso
    )
    # prices = extract(
    # config.octopus.api_key, config., from_iso, to_iso
    # )
    electricity = transform("electricity", electricity, rate_data["electricity"])
    load(connection, config, electricity, "electricity")
    gas, rate_data["gas"]["standing_charge"] = extract(
        config.octopus.api_key, config.gas, from_iso, to_iso
    )
    gas = transform("gas", gas, rate_data["gas"])
    load(connection, config, gas, "gas")


if __name__ == "__main__":
    main()
