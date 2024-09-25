from binance_archiver import launch_data_listener
from binance_archiver.archiver_daemon import Observer
from load_config import load_config
import time

class ConcreteObserver(Observer):
    def update(self, message):
        print(f"message: {message}!")
        ...

if __name__ == '__main__':

    sample_observer = ConcreteObserver()

    data_listener = launch_data_listener(
        config=load_config('test_config.json'),
        init_observers=[sample_observer]
    )