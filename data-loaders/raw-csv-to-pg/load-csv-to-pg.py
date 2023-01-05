import click
import pandas
import os

from sqlalchemy import create_engine

class EIA930File(object):
    FILE_COLUMN_MAPPINGS = {
        'balance': {
            'Balancing Authority': 'balancing_authority',
            'UTC Time at End of Hour': 'utc_end_time',
            'Demand Forecast (MW)': 'demand_forecast_mw',
            'Demand (MW)': 'demand_mw',
            'Net Generation (MW)': 'net_generation_mw',
            'Total Interchange (MW)': 'total_interchange_mw',
            'Demand (MW) (Adjusted)': 'demand_adjusted_mw',
            'Net Generation (MW) (Adjusted)': 'net_generation_adjusted_mw',
            'Net Generation (MW) from Coal': 'net_generation_coal_mw',
            'Net Generation (MW) from Natural Gas': 'net_generation_natural_gas_mw',
            'Net Generation (MW) from Nuclear': 'net_generation_nuclear_mw',
            'Net Generation (MW) from All Petroleum Products': 'net_generation_petroleum_mw',
            'Net Generation (MW) from Hydropower and Pumped Storage': 'net_generation_hydropower_mw',
            'Net Generation (MW) from Solar': 'net_generation_solar_mw',
            'Net Generation (MW) from Wind': 'net_generation_wind_mw',
            'Net Generation (MW) from Other Fuel Sources': 'net_generation_other_mw',
            'Net Generation (MW) from Unknown Fuel Sources': 'net_generation_unknown_mw'
        },
        'interchange': {
            'Balancing Authority': 'balancing_authority',
            'Directly Interconnected Balancing Authority': 'connected_balancing_authority',
            'Interchange (MW)': 'interchange_amount_mw',
            'UTC Time at End of Hour': 'utc_end_time'
        }
    }
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.filename = self.filepath.split('.csv')[0].split('/')[-1]
        self.column_mapping = self.FILE_COLUMN_MAPPINGS[self.get_file_type()]

    def get_file_type(self) -> str:
        if 'BALANCE' in self.filepath:
            return 'balance'
        if 'INTERCHANGE' in self.filepath:
            return 'interchange'
        return None

    

    def load_file(self):
        self.df = pandas.read_csv(
            self.filepath,
            usecols=list(self.column_mapping.keys()),
            thousands=','
        )
    
    def transform_file(self):
        # Rename columns according to column mapping
        self.df = self.df.rename(columns={ ck : self.column_mapping[ck] for ck in self.column_mapping.keys() })

        # Fill in zeros for null values
        self.df = self.df.fillna(0)

        # Convert datetime columns to datetime
        self.df['utc_end_time'] = pandas.to_datetime(self.df['utc_end_time'])

    def export_dataframe(self) -> pandas.DataFrame:
        return self.df

class PostgresClient(object):
    def __init__(self, conn_string: str):
        self.engine = create_engine(conn_string)

    def load_dataframe(self, file: EIA930File):
        file.export_dataframe().to_sql(
            f"energy_{file.get_file_type()}",
            self.engine,
            schema='staging',
            index=False, # Not copying over the index
            if_exists='append', # Only in charge of loading the data
            chunksize=100000
        )


def get_environment() -> dict:
    POSTGRES_CONNECTION_STRING = os.getenv('POSTGRES_CONNECTION_STRING')
    if POSTGRES_CONNECTION_STRING is None:
        raise Exception('Must set "POSTGRES_CONNECTION_STRING" environment variable')
    return { "POSTGRES_CONNECTION_STRING": POSTGRES_CONNECTION_STRING }


@click.command()
@click.argument('filepath')
@click.option('--table',
    type=click.Choice(['BALANCE', 'INTERCHANGE'], case_sensitive=False),
    help='Which EIA table is being loaded. If not provided, is inferred from file name')
def cli(filepath: str, table: str):
    # Retrieve any environment variables
    environment = get_environment()
    # Load / transform / Upload steps
    file_object = EIA930File(filepath)
    file_object.load_file()
    file_object.transform_file()

    pg_client = PostgresClient(environment["POSTGRES_CONNECTION_STRING"])
    pg_client.load_dataframe(file_object)


if __name__ == '__main__':
    cli()