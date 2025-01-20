import os

from binance_archiver import conduct_csv_files_data_quality_analysis, \
    conduct_whole_directory_of_csvs_data_quality_analysis

if __name__ == '__main__':

    csv_nest_directory = os.path.join(
        os.path.expanduser("~"),
        "Documents",
        "binance_archival_data"
    ).replace('\\', '/')

    csv_paths = [
        f'{csv_nest_directory}/binance_trade_stream_usd_m_futures_trxusdt_15-01-2025.csv',
        f'{csv_nest_directory}/binance_trade_stream_spot_trxusdt_15-01-2025.csv'
    ]

    conduct_csv_files_data_quality_analysis(csv_paths=csv_paths)
    conduct_whole_directory_of_csvs_data_quality_analysis(csv_nest_directory=csv_nest_directory)
