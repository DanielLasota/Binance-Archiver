import json
import os
import time
from orderbook_level_2_listener.orderbook_level_2_listener import OrderbookDaemon
from orderbook_level_2_listener.market_enum import Market
from dotenv import load_dotenv


class DaemonManager:
    def __init__(
            self,
            config_path: str = 'config.json',
            env_path: str = '.env',
            dump_path: str = '',
            should_csv_be_removed_after_zip: bool = True,
            should_zip_be_removed_after_upload: bool = True,
            should_zip_be_sent: bool = True
    ) -> None:
        self.config_path = config_path
        self.env_path = env_path
        self.dump_path = dump_path
        self.daemons = []
        self.should_csv_be_removed_after_zip = should_csv_be_removed_after_zip
        self.should_zip_be_removed_after_upload = should_zip_be_removed_after_upload
        self.should_zip_be_sent = should_zip_be_sent

    def load_config(self):
        with open(self.config_path, 'r') as file:
            return json.load(file)

    def start_daemons(self):
        if self.dump_path != '' and not os.path.exists(self.dump_path):
            os.makedirs(self.dump_path)

        load_dotenv(self.env_path)
        config = self.load_config()
        listen_duration = config['daemons']['listen_duration']

        for market_type, instruments in config['daemons']['markets'].items():
            market_enum = Market[market_type.upper()]
            for instrument in instruments:
                daemon = OrderbookDaemon(
                    azure_blob_parameters_with_key=os.environ.get('AZURE_BLOB_PARAMETERS_WITH_KEY'),
                    container_name=os.environ.get('CONTAINER_NAME'),
                    should_csv_be_removed_after_zip=self.should_csv_be_removed_after_zip,
                    should_zip_be_removed_after_upload=self.should_zip_be_removed_after_upload,
                    should_zip_be_sent=self.should_zip_be_sent
                )
                daemon.run(
                    instrument=instrument,
                    market=market_enum,
                    single_file_listen_duration_in_seconds=listen_duration,
                    dump_path=self.dump_path
                )
                self.daemons.append(daemon)

    def stop_daemons(self):
        for daemon in self.daemons:
            # daemon.close_all() # under implementation
            pass
        print("Stopped all daemons")

    def run(self):
        self.start_daemons()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_daemons()