from dataclasses import dataclass
from datetime import timedelta

from t_tech.invest.grpc import marketdata_pb2


@dataclass(frozen=True)
class GrowthCandleInterval:
    label: str
    api_interval: int
    lookback: timedelta


GROWTH_CANDLE_INTERVALS: dict[str, GrowthCandleInterval] = {
    "5 секунд": GrowthCandleInterval(
        label="5 секунд",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_5_SEC,
        lookback=timedelta(minutes=2),
    ),
    "10 секунд": GrowthCandleInterval(
        label="10 секунд",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_10_SEC,
        lookback=timedelta(minutes=2),
    ),
    "30 секунд": GrowthCandleInterval(
        label="30 секунд",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_30_SEC,
        lookback=timedelta(minutes=5),
    ),
    "1 минута": GrowthCandleInterval(
        label="1 минута",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_1_MIN,
        lookback=timedelta(minutes=10),
    ),
    "2 минуты": GrowthCandleInterval(
        label="2 минуты",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_2_MIN,
        lookback=timedelta(minutes=20),
    ),
    "3 минуты": GrowthCandleInterval(
        label="3 минуты",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_3_MIN,
        lookback=timedelta(minutes=30),
    ),
    "5 минут": GrowthCandleInterval(
        label="5 минут",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_5_MIN,
        lookback=timedelta(hours=1),
    ),
    "10 минут": GrowthCandleInterval(
        label="10 минут",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_10_MIN,
        lookback=timedelta(hours=2),
    ),
    "15 минут": GrowthCandleInterval(
        label="15 минут",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_15_MIN,
        lookback=timedelta(hours=3),
    ),
    "30 минут": GrowthCandleInterval(
        label="30 минут",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_30_MIN,
        lookback=timedelta(hours=6),
    ),
    "1 час": GrowthCandleInterval(
        label="1 час",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_HOUR,
        lookback=timedelta(hours=12),
    ),
    "2 часа": GrowthCandleInterval(
        label="2 часа",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_2_HOUR,
        lookback=timedelta(days=1),
    ),
    "4 часа": GrowthCandleInterval(
        label="4 часа",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_4_HOUR,
        lookback=timedelta(days=2),
    ),
    "1 день": GrowthCandleInterval(
        label="1 день",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_DAY,
        lookback=timedelta(days=10),
    ),
    "1 неделя": GrowthCandleInterval(
        label="1 неделя",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_WEEK,
        lookback=timedelta(days=60),
    ),
    "1 месяц": GrowthCandleInterval(
        label="1 месяц",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_MONTH,
        lookback=timedelta(days=400),
    ),
}


def get_growth_candle_interval(label: str) -> GrowthCandleInterval:
    if label not in GROWTH_CANDLE_INTERVALS:
        raise ValueError(f"Неизвестный интервал расчёта роста: {label}")

    return GROWTH_CANDLE_INTERVALS[label]
