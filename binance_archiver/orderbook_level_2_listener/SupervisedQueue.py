import copy
import json
import pprint
import threading
import time
from queue import Queue
from typing import Any, List, Dict

from binance_archiver.orderbook_level_2_listener.stream_age_enum import StreamAge
from binance_archiver.orderbook_level_2_listener.stream_type_enum import StreamType


class SupervisedQueue:
    def __init__(self, logger, stream_type: StreamType):
        self.logger = logger
        self.queue = Queue()
        self.stream_type = stream_type
        self.currently_accepted_stream_age = StreamAge.OLD
        self.lock = threading.Lock()

        choose = {
            StreamType.DIFFERENCE_DEPTH: self._put_difference_depth_message,
            StreamType.TRADE: self._put_trade_message
        }

        self.put = choose.get(stream_type, None)

        if stream_type == StreamType.DIFFERENCE_DEPTH:
            self.did_websockets_switch_successfully = False

            self._two_last_throws = {
                StreamAge.NEW: [],
                StreamAge.OLD: []
            }

    def _add_to_compare(self, id_data, message):
        message_dict = json.loads(message)

        if id_data['stream_age'] == StreamAge.NEW:
            if len(self._two_last_throws[StreamAge.NEW]) > 0:
                if message_dict['data']['E'] > self._two_last_throws[StreamAge.NEW][-1]['data']['E'] + 10:
                    self._two_last_throws[StreamAge.NEW] = []
            self._two_last_throws[StreamAge.NEW].append(message_dict)

        if id_data['stream_age'] == StreamAge.OLD:
            if len(self._two_last_throws[StreamAge.OLD]) > 0:
                if message_dict['data']['E'] > self._two_last_throws[StreamAge.OLD][-1]['data']['E'] + 10:
                    self._two_last_throws[StreamAge.OLD] = []
            self._two_last_throws[StreamAge.OLD].append(message_dict)

        # if id_data['stream_age'] == StreamAge.OLD:
        #     print(f'states ov self._last_timestamp_input[OLD]: {time.time_ns()}')
        #     for _ in self._two_last_throws[StreamAge.OLD]:
        #         print(_)
        #
        # if id_data['stream_age'] == StreamAge.NEW:
        #     print(f'states ov self._last_timestamp_input[NEW]: {time.time_ns()}')
        #     for _ in self._two_last_throws[StreamAge.NEW]:
        #         print(_)

        if self._two_last_throws[StreamAge.NEW] == self._two_last_throws[StreamAge.OLD]:
            print('huj VVV')
            pprint.pprint(self._two_last_throws)
            print('huj ^^^')

        amount_of_listened_pairs = id_data['pairs_amount']
        do_they_match = self._compare_two_last_throws(amount_of_listened_pairs, self._two_last_throws)

        if do_they_match is True:
            print('ugabuga')

    @staticmethod
    def _compare_two_last_throws(amount_of_listened_pairs: int, two_last_throws: Dict) -> bool:

        if len(two_last_throws[StreamAge.NEW]) == len(two_last_throws[StreamAge.OLD]) == amount_of_listened_pairs:

            print('lens are equal')

            pprint.pprint(two_last_throws)

            copied_two_last_throws = copy.deepcopy(two_last_throws)

            sorted_and_copied_two_last_throws = SupervisedQueue.sort_entries_by_symbol(copied_two_last_throws)

            for stream_age in sorted_and_copied_two_last_throws:
                for entry in sorted_and_copied_two_last_throws[stream_age]:
                    entry['data'].pop('E', None)

            if sorted_and_copied_two_last_throws[StreamAge.NEW] == sorted_and_copied_two_last_throws[StreamAge.OLD]:
                return True

        return False

    @staticmethod
    def sort_entries_by_symbol(two_last_throws: Dict) -> Dict:
        for stream_age in two_last_throws:
            two_last_throws[stream_age].sort(key=lambda entry: entry['data']['s'])
        return two_last_throws

    def _put_difference_depth_message(self, id_data, message):

        with self.lock:
            self._add_to_compare(id_data, message)

            if id_data['stream_age'] == self.currently_accepted_stream_age:
                self.queue.put(message)

    def _put_trade_message(self, message):
        self.queue.put(message)

    def get(self) -> Any:
        message = self.queue.get()
        return message

    def get_nowait(self) -> Any:
        message = self.queue.get_nowait()
        return message

    def clear(self) -> None:
        self.queue.queue.clear()

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()

    def set_websocket_switch_indicator(self, did_websockets_switch_successfully):
        self.did_websockets_switch_successfully = did_websockets_switch_successfully

'''
states ov self._last_timestamp_input[NEW]: 1720337869529797400
{'stream': 'dotusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'DOTUSDT', 'U': 7871863945, 'u': 7871863947, 'b': [['6.19800000', '1816.61000000'], ['6.19300000', '1592.79000000']], 'a': [['6.20800000', '1910.71000000']]}}
{'stream': 'adausdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'ADAUSDT', 'U': 8823504433, 'u': 8823504452, 'b': [['0.36440000', '46561.40000000'], ['0.36430000', '76839.90000000'], ['0.36400000', '76688.60000000'], ['0.36390000', '106235.50000000'], ['0.36370000', '35413.10000000']], 'a': [['0.36450000', '16441.60000000'], ['0.36460000', '20497.10000000'], ['0.36470000', '39808.80000000'], ['0.36480000', '75106.10000000'], ['0.36900000', '32.90000000'], ['0.37120000', '361.70000000']]}}
{'stream': 'trxusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'TRXUSDT', 'U': 4609985365, 'u': 4609985365, 'b': [['0.12984000', '123840.00000000']], 'a': []}}
{'stream': 'dogeusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'DOGEUSDT', 'U': 9782975037, 'u': 9782975045, 'b': [['0.10949000', '105865.00000000'], ['0.10926000', '652194.00000000']], 'a': [['0.10950000', '14366.00000000'], ['0.10954000', '106741.00000000'], ['0.10955000', '51716.00000000'], ['0.10956000', '261498.00000000'], ['0.10960000', '120620.00000000']]}}
{'stream': 'xrpusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'XRPUSDT', 'U': 11531135570, 'u': 11531135570, 'b': [], 'a': [['0.43570000', '27020.00000000']]}}
{'stream': 'solusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'SOLUSDT', 'U': 12106885551, 'u': 12106885569, 'b': [['138.48000000', '156.60600000'], ['138.44000000', '96.54900000'], ['138.38000000', '78.20100000'], ['138.35000000', '233.22200000'], ['138.32000000', '235.42700000'], ['138.31000000', '43.82100000'], ['137.24000000', '184.76300000'], ['137.19000000', '26.35000000']], 'a': [['138.49000000', '123.38900000'], ['138.51000000', '885.48000000'], ['138.54000000', '227.64600000'], ['138.72000000', '256.16800000'], ['138.78000000', '184.09200000']]}}
{'stream': 'ethusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'ETHUSDT', 'U': 33934245010, 'u': 33934245106, 'b': [['2998.40000000', '62.79810000'], ['2998.39000000', '0.00000000'], ['2998.38000000', '0.01120000'], ['2998.37000000', '0.10050000'], ['2998.35000000', '0.00000000'], ['2998.28000000', '20.58150000'], ['2998.19000000', '13.67470000'], ['2998.13000000', '6.51130000'], ['2998.07000000', '24.48550000'], ['2997.98000000', '0.00260000'], ['2997.65000000', '1.09180000'], ['2997.53000000', '1.18680000'], ['2996.91000000', '8.39610000'], ['2996.80000000', '5.20720000'], ['2996.60000000', '8.34030000'], ['2996.42000000', '2.76320000'], ['2996.41000000', '0.00000000'], ['2996.30000000', '1.79230000'], ['2996.22000000', '0.03380000'], ['2996.21000000', '0.97490000'], ['2996.12000000', '0.00000000'], ['2996.11000000', '1.88170000'], ['2995.86000000', '0.00000000'], ['2995.79000000', '0.00000000'], ['2995.73000000', '0.00000000'], ['2995.15000000', '0.00000000'], ['2993.15000000', '0.05220000'], ['2989.76000000', '0.51900000'], ['2988.80000000', '0.00000000'], ['2986.51000000', '0.01000000'], ['2986.18000000', '0.00000000']], 'a': [['2998.41000000', '7.60630000'], ['2998.44000000', '0.10000000'], ['2998.52000000', '0.00000000'], ['2998.58000000', '1.42850000'], ['2998.60000000', '4.85000000'], ['2998.62000000', '0.00500000'], ['2998.68000000', '0.10000000'], ['2998.87000000', '0.00000000'], ['2998.90000000', '0.00000000'], ['2999.00000000', '7.63080000'], ['2999.01000000', '0.00330000'], ['2999.04000000', '0.10330000'], ['2999.07000000', '0.00300000'], ['2999.24000000', '9.73500000'], ['2999.25000000', '0.00000000'], ['2999.33000000', '0.00000000'], ['2999.34000000', '21.37360000'], ['2999.38000000', '0.00000000'], ['2999.47000000', '0.00000000'], ['2999.49000000', '0.00630000'], ['2999.64000000', '10.21620000'], ['2999.70000000', '1.67630000'], ['2999.88000000', '0.40000000'], ['2999.89000000', '0.63410000'], ['2999.92000000', '0.00300000'], ['3000.06000000', '0.01660000'], ['3000.08000000', '14.00000000'], ['3000.09000000', '1.67350000'], ['3000.19000000', '3.71410000'], ['3000.42000000', '0.18700000'], ['3000.88000000', '1.56410000'], ['3001.14000000', '4.70850000'], ['3001.30000000', '6.55950000'], ['3001.75000000', '1.49740000'], ['3002.02000000', '7.55810000'], ['3004.70000000', '0.00000000'], ['3005.90000000', '0.00000000'], ['3006.86000000', '0.55400000']]}}
{'stream': 'ltcusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'LTCUSDT', 'U': 7991172426, 'u': 7991172426, 'b': [], 'a': [['64.10000000', '298.38200000']]}}
{'stream': 'btcusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'BTCUSDT', 'U': 48554975753, 'u': 48554975800, 'b': [['57181.09000000', '7.60396000'], ['57178.52000000', '0.00699000'], ['57152.91000000', '0.06386000'], ['57152.16000000', '0.20984000'], ['57143.19000000', '0.00000000'], ['57134.22000000', '0.36475000'], ['57128.48000000', '0.00000000'], ['57128.23000000', '0.50195000'], ['57111.11000000', '0.42030000'], ['57076.09000000', '0.01000000'], ['57073.60000000', '0.00556000'], ['56725.56000000', '0.01309000'], ['51462.00000000', '0.00000000'], ['33000.00000000', '10.71596000']], 'a': [['57181.10000000', '0.47325000'], ['57181.11000000', '0.04589000'], ['57186.65000000', '0.01572000'], ['57190.00000000', '0.13980000'], ['57191.02000000', '0.21576000'], ['57193.11000000', '0.00000000'], ['57206.08000000', '0.27728000'], ['57207.50000000', '0.13980000'], ['57208.00000000', '0.07820000'], ['57208.92000000', '0.99213000'], ['57209.96000000', '0.00000000'], ['57214.70000000', '0.00000000'], ['57217.16000000', '0.49220000'], ['57217.19000000', '0.00000000'], ['57227.02000000', '5.29777000'], ['57233.62000000', '0.03166000'], ['57263.76000000', '0.00000000'], ['57281.10000000', '0.07983000'], ['57286.11000000', '0.00556000'], ['58289.66000000', '0.08747000']]}}
{'stream': 'shibusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'SHIBUSDT', 'U': 4843890060, 'u': 4843890066, 'b': [['0.00001627', '871061097.00'], ['0.00001626', '2828244039.00'], ['0.00001623', '2084161183.00']], 'a': [['0.00001629', '2394595980.00']]}}
{'stream': 'avaxusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'AVAXUSDT', 'U': 5986972005, 'u': 5986972006, 'b': [], 'a': [['26.36000000', '1630.94000000'], ['26.51000000', '43.74000000']]}}
{'stream': 'bnbusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'BNBUSDT', 'U': 9871397622, 'u': 9871397622, 'b': [], 'a': [['526.20000000', '1.46400000']]}}

states ov self._last_timestamp_input[OLD]: 1720337869551585400
{'stream': 'dotusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'DOTUSDT', 'U': 7871863945, 'u': 7871863947, 'b': [['6.19800000', '1816.61000000'], ['6.19300000', '1592.79000000']], 'a': [['6.20800000', '1910.71000000']]}}
{'stream': 'adausdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'ADAUSDT', 'U': 8823504433, 'u': 8823504452, 'b': [['0.36440000', '46561.40000000'], ['0.36430000', '76839.90000000'], ['0.36400000', '76688.60000000'], ['0.36390000', '106235.50000000'], ['0.36370000', '35413.10000000']], 'a': [['0.36450000', '16441.60000000'], ['0.36460000', '20497.10000000'], ['0.36470000', '39808.80000000'], ['0.36480000', '75106.10000000'], ['0.36900000', '32.90000000'], ['0.37120000', '361.70000000']]}}
{'stream': 'trxusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'TRXUSDT', 'U': 4609985365, 'u': 4609985365, 'b': [['0.12984000', '123840.00000000']], 'a': []}}
{'stream': 'dogeusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'DOGEUSDT', 'U': 9782975037, 'u': 9782975045, 'b': [['0.10949000', '105865.00000000'], ['0.10926000', '652194.00000000']], 'a': [['0.10950000', '14366.00000000'], ['0.10954000', '106741.00000000'], ['0.10955000', '51716.00000000'], ['0.10956000', '261498.00000000'], ['0.10960000', '120620.00000000']]}}
{'stream': 'xrpusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'XRPUSDT', 'U': 11531135570, 'u': 11531135570, 'b': [], 'a': [['0.43570000', '27020.00000000']]}}
{'stream': 'solusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'SOLUSDT', 'U': 12106885551, 'u': 12106885569, 'b': [['138.48000000', '156.60600000'], ['138.44000000', '96.54900000'], ['138.38000000', '78.20100000'], ['138.35000000', '233.22200000'], ['138.32000000', '235.42700000'], ['138.31000000', '43.82100000'], ['137.24000000', '184.76300000'], ['137.19000000', '26.35000000']], 'a': [['138.49000000', '123.38900000'], ['138.51000000', '885.48000000'], ['138.54000000', '227.64600000'], ['138.72000000', '256.16800000'], ['138.78000000', '184.09200000']]}}
{'stream': 'ethusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'ETHUSDT', 'U': 33934245010, 'u': 33934245106, 'b': [['2998.40000000', '62.79810000'], ['2998.39000000', '0.00000000'], ['2998.38000000', '0.01120000'], ['2998.37000000', '0.10050000'], ['2998.35000000', '0.00000000'], ['2998.28000000', '20.58150000'], ['2998.19000000', '13.67470000'], ['2998.13000000', '6.51130000'], ['2998.07000000', '24.48550000'], ['2997.98000000', '0.00260000'], ['2997.65000000', '1.09180000'], ['2997.53000000', '1.18680000'], ['2996.91000000', '8.39610000'], ['2996.80000000', '5.20720000'], ['2996.60000000', '8.34030000'], ['2996.42000000', '2.76320000'], ['2996.41000000', '0.00000000'], ['2996.30000000', '1.79230000'], ['2996.22000000', '0.03380000'], ['2996.21000000', '0.97490000'], ['2996.12000000', '0.00000000'], ['2996.11000000', '1.88170000'], ['2995.86000000', '0.00000000'], ['2995.79000000', '0.00000000'], ['2995.73000000', '0.00000000'], ['2995.15000000', '0.00000000'], ['2993.15000000', '0.05220000'], ['2989.76000000', '0.51900000'], ['2988.80000000', '0.00000000'], ['2986.51000000', '0.01000000'], ['2986.18000000', '0.00000000']], 'a': [['2998.41000000', '7.60630000'], ['2998.44000000', '0.10000000'], ['2998.52000000', '0.00000000'], ['2998.58000000', '1.42850000'], ['2998.60000000', '4.85000000'], ['2998.62000000', '0.00500000'], ['2998.68000000', '0.10000000'], ['2998.87000000', '0.00000000'], ['2998.90000000', '0.00000000'], ['2999.00000000', '7.63080000'], ['2999.01000000', '0.00330000'], ['2999.04000000', '0.10330000'], ['2999.07000000', '0.00300000'], ['2999.24000000', '9.73500000'], ['2999.25000000', '0.00000000'], ['2999.33000000', '0.00000000'], ['2999.34000000', '21.37360000'], ['2999.38000000', '0.00000000'], ['2999.47000000', '0.00000000'], ['2999.49000000', '0.00630000'], ['2999.64000000', '10.21620000'], ['2999.70000000', '1.67630000'], ['2999.88000000', '0.40000000'], ['2999.89000000', '0.63410000'], ['2999.92000000', '0.00300000'], ['3000.06000000', '0.01660000'], ['3000.08000000', '14.00000000'], ['3000.09000000', '1.67350000'], ['3000.19000000', '3.71410000'], ['3000.42000000', '0.18700000'], ['3000.88000000', '1.56410000'], ['3001.14000000', '4.70850000'], ['3001.30000000', '6.55950000'], ['3001.75000000', '1.49740000'], ['3002.02000000', '7.55810000'], ['3004.70000000', '0.00000000'], ['3005.90000000', '0.00000000'], ['3006.86000000', '0.55400000']]}}
{'stream': 'ltcusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'LTCUSDT', 'U': 7991172426, 'u': 7991172426, 'b': [], 'a': [['64.10000000', '298.38200000']]}}
{'stream': 'btcusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'BTCUSDT', 'U': 48554975753, 'u': 48554975800, 'b': [['57181.09000000', '7.60396000'], ['57178.52000000', '0.00699000'], ['57152.91000000', '0.06386000'], ['57152.16000000', '0.20984000'], ['57143.19000000', '0.00000000'], ['57134.22000000', '0.36475000'], ['57128.48000000', '0.00000000'], ['57128.23000000', '0.50195000'], ['57111.11000000', '0.42030000'], ['57076.09000000', '0.01000000'], ['57073.60000000', '0.00556000'], ['56725.56000000', '0.01309000'], ['51462.00000000', '0.00000000'], ['33000.00000000', '10.71596000']], 'a': [['57181.10000000', '0.47325000'], ['57181.11000000', '0.04589000'], ['57186.65000000', '0.01572000'], ['57190.00000000', '0.13980000'], ['57191.02000000', '0.21576000'], ['57193.11000000', '0.00000000'], ['57206.08000000', '0.27728000'], ['57207.50000000', '0.13980000'], ['57208.00000000', '0.07820000'], ['57208.92000000', '0.99213000'], ['57209.96000000', '0.00000000'], ['57214.70000000', '0.00000000'], ['57217.16000000', '0.49220000'], ['57217.19000000', '0.00000000'], ['57227.02000000', '5.29777000'], ['57233.62000000', '0.03166000'], ['57263.76000000', '0.00000000'], ['57281.10000000', '0.07983000'], ['57286.11000000', '0.00556000'], ['58289.66000000', '0.08747000']]}}
{'stream': 'shibusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'SHIBUSDT', 'U': 4843890060, 'u': 4843890066, 'b': [['0.00001627', '871061097.00'], ['0.00001626', '2828244039.00'], ['0.00001623', '2084161183.00']], 'a': [['0.00001629', '2394595980.00']]}}
{'stream': 'avaxusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'AVAXUSDT', 'U': 5986972005, 'u': 5986972006, 'b': [], 'a': [['26.36000000', '1630.94000000'], ['26.51000000', '43.74000000']]}}
{'stream': 'bnbusdt@depth@100ms', 'data': {'e': 'depthUpdate', 'E': 1720337869216, 's': 'BNBUSDT', 'U': 9871397622, 'u': 9871397622, 'b': [], 'a': [['526.20000000', '1.46400000']]}}
matches
'''
