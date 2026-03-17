from threading import Lock

APP_STATE = {
    "google_business": {
        "running": False,
        "stop": False,
        "current": 0,
        "total": 0,
        "results": [],
        "logs": [],
        "thread": None,
    },
    "social_lookup": {
        "running": False,
        "stop": False,
        "current": 0,
        "total": 0,
        "results": [],
        "logs": [],
        "thread": None,
    },
}

STATE_LOCK = Lock()


def reset_mode(mode: str):
    with STATE_LOCK:
        APP_STATE[mode]["running"] = False
        APP_STATE[mode]["stop"] = False
        APP_STATE[mode]["current"] = 0
        APP_STATE[mode]["total"] = 0
        APP_STATE[mode]["results"] = []
        APP_STATE[mode]["logs"] = []
        APP_STATE[mode]["thread"] = None


def add_log(mode: str, message: str):
    with STATE_LOCK:
        APP_STATE[mode]["logs"].append(message)


def set_running(mode: str, value: bool):
    with STATE_LOCK:
        APP_STATE[mode]["running"] = value


def set_stop(mode: str, value: bool):
    with STATE_LOCK:
        APP_STATE[mode]["stop"] = value


def is_stopped(mode: str) -> bool:
    with STATE_LOCK:
        return APP_STATE[mode]["stop"]


def set_total(mode: str, total: int):
    with STATE_LOCK:
        APP_STATE[mode]["total"] = total


def increment_current(mode: str):
    with STATE_LOCK:
        APP_STATE[mode]["current"] += 1


def add_result(mode: str, row: dict):
    with STATE_LOCK:
        APP_STATE[mode]["results"].append(row)


def get_mode_state(mode: str):
    with STATE_LOCK:
        return {
            "running": APP_STATE[mode]["running"],
            "current": APP_STATE[mode]["current"],
            "total": APP_STATE[mode]["total"],
            "results": list(APP_STATE[mode]["results"]),
            "logs": list(APP_STATE[mode]["logs"]),
        }