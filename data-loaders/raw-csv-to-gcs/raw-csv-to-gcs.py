import click
import pandas

class EIA930File(object):
    FILE_COLUMN_MAPPINGS = {
        'BALANCE': {
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
        'INTERCHANGE': {
            'Balancing Authority': 'balancing_authority',
            'Directly Interconnected Balancing Authority': 'connected_balancing_authority',
            'Interchange (MW)': 'interchange_amount_mw',
            'UTC Time at End of Hour': 'utc_end_time'
        }
    }
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.filename = self.filepath.split('.csv')[0].split('/')[-1]
        self.file_type = self.get_file_type(self.filepath)
        self.column_mapping = self.FILE_COLUMN_MAPPINGS[self.file_type]

    def get_file_type(self, filepath) -> str:
        if 'BALANCE' in filepath:
            return 'BALANCE'
        if 'INTERCHANGE' in filepath:
            return 'INTERCHANGE'
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

class GCStorageFile(object):
    def __init__(self, gcs_bucket: str, file: EIA930File):
        self.file = file
        self.gcs_bucket = gcs_bucket
        self.source = 'EIA930'

    def get_gcs_path_for_file(self) -> str:
        timeperiod = self.file.filename.split(self.source)[1].split(self.file.file_type)[1]
        return f"gs://{self.gcs_bucket}/{self.source}/{self.file.file_type}{timeperiod}.parquet"

    def save_dataframe_to_storage(self):
        gcs_path = self.get_gcs_path_for_file()
        self.file.export_dataframe().to_parquet(gcs_path)


@click.command()
@click.argument('filepath')
@click.argument('gcs_bucket')
@click.option('--table',
    type=click.Choice(['BALANCE', 'INTERCHANGE'], case_sensitive=False),
    help='Which EIA table is being loaded. If not provided, is inferred from file name')
def cli(filepath: str, gcs_bucket: str, table: str):
    # Load / transform / Upload steps
    file_object = EIA930File(filepath)
    file_object.load_file()
    file_object.transform_file()

    file_on_storage = GCStorageFile(gcs_bucket, file_object)
    file_on_storage.save_dataframe_to_storage()



if __name__ == '__main__':
    cli()