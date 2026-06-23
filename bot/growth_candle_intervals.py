from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from t_tech.invest.grpc import marketdata_pb2


@dataclass(frozen=True)
class GrowthCandleInterval:
    label: str
    api_interval: int
    lookback: timedelta
    duration: timedelta | None


GROWTH_CANDLE_INTERVALS: dict[str, GrowthCandleInterval] = {
    "5 секунд": GrowthCandleInterval(
        label="5 секунд",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_5_SEC,
        lookback=timedelta(minutes=2),
        duration=timedelta(seconds=5),
    ),
    "10 секунд": GrowthCandleInterval(
        label="10 секунд",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_10_SEC,
        lookback=timedelta(minutes=2),
        duration=timedelta(seconds=10),
    ),
    "30 секунд": GrowthCandleInterval(
        label="30 секунд",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_30_SEC,
        lookback=timedelta(minutes=5),
        duration=timedelta(seconds=30),
    ),
    "1 минута": GrowthCandleInterval(
        label="1 минута",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_1_MIN,
        lookback=timedelta(minutes=10),
        duration=timedelta(minutes=1),
    ),
    "2 минуты": GrowthCandleInterval(
        label="2 минуты",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_2_MIN,
        lookback=timedelta(minutes=20),
        duration=timedelta(minutes=2),
    ),
    "3 минуты": GrowthCandleInterval(
        label="3 минуты",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_3_MIN,
        lookback=timedelta(minutes=30),
        duration=timedelta(minutes=3),
    ),
    "5 минут": GrowthCandleInterval(
        label="5 минут",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_5_MIN,
        lookback=timedelta(hours=1),
        duration=timedelta(minutes=5),
    ),
    "10 минут": GrowthCandleInterval(
        label="10 минут",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_10_MIN,
        lookback=timedelta(hours=2),
        duration=timedelta(minutes=10),
    ),
    "15 минут": GrowthCandleInterval(
        label="15 минут",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_15_MIN,
        lookback=timedelta(hours=3),
        duration=timedelta(minutes=15),
    ),
    "30 минут": GrowthCandleInterval(
        label="30 минут",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_30_MIN,
        lookback=timedelta(hours=6),
        duration=timedelta(minutes=30),
    ),
    "1 час": GrowthCandleInterval(
        label="1 час",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_HOUR,
        lookback=timedelta(hours=12),
        duration=timedelta(hours=1),
    ),
    "2 часа": GrowthCandleInterval(
        label="2 часа",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_2_HOUR,
        lookback=timedelta(days=1),
        duration=timedelta(hours=2),
    ),
    "4 часа": GrowthCandleInterval(
        label="4 часа",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_4_HOUR,
        lookback=timedelta(days=2),
        duration=timedelta(hours=4),
    ),
    "1 день": GrowthCandleInterval(
        label="1 день",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_DAY,
        lookback=timedelta(days=10),
        duration=timedelta(days=1),
    ),
    "1 неделя": GrowthCandleInterval(
        label="1 неделя",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_WEEK,
        lookback=timedelta(days=60),
        duration=timedelta(days=7),
    ),
    "1 месяц": GrowthCandleInterval(
        label="1 месяц",
        api_interval=marketdata_pb2.CANDLE_INTERVAL_MONTH,
        lookback=timedelta(days=400),
        duration=None,
    ),
}


def get_growth_candle_interval(label: str) -> GrowthCandleInterval:
    if label not in GROWTH_CANDLE_INTERVALS:
        raise ValueError(f"Неизвестный интервал расчёта роста: {label}")

    return GROWTH_CANDLE_INTERVALS[label]


def _add_one_month(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime должен быть timezone-aware.")

    if value.month == 12:
        return value.replace(year=value.year + 1, month=1)

    return value.replace(month=value.month + 1)


def is_candle_cache_actual(
    interval: GrowthCandleInterval,
    candle_time_utc: datetime,
    now_utc: datetime,
) -> bool:
    if candle_time_utc.tzinfo is None:
        raise ValueError("candle_time_utc должен быть timezone-aware.")

    if now_utc.tzinfo is None:
        raise ValueError("now_utc должен быть timezone-aware.")

    candle_time_utc = candle_time_utc.astimezone(timezone.utc)
    now_utc = now_utc.astimezone(timezone.utc)

    if now_utc < candle_time_utc:
        return False

    if interval.duration is None:
        next_candle_time = _add_one_month(candle_time_utc)
    else:
        next_candle_time = candle_time_utc + interval.duration

    return candle_time_utc <= now_utc < next_candle_time
