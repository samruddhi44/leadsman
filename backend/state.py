from threading import Lock

APP_STATE = {
    "google_business": {
        "running": False,
        "stop": False,
        "current": 0,
        "total": 0,
        "results": [],
        "last_results": [],
        "logs": [],
        "thread": None,
    },
    "social_lookup": {
        "running": False,
        "stop": False,
        "current": 0,
        "total": 0,
        "results": [],
        "last_results": [],
        "logs": [],
        "thread": None,
    },
}

VALID_MODES = tuple(APP_STATE.keys())
STATE_LOCK = Lock()


def ensure_mode(mode: str) -> str:
    if mode not in APP_STATE:
        raise ValueError(f"Unsupported mode: {mode}")
    return mode


def reset_mode(mode: str):
    mode = ensure_mode(mode)
    with STATE_LOCK:
        state = APP_STATE[mode]
        state["running"] = False
        state["stop"] = False
        state["current"] = 0
        state["total"] = 0
        state["results"] = []
        state["last_results"] = []
        state["logs"] = []
        state["thread"] = None


def add_log(mode: str, message: str):
    mode = ensure_mode(mode)
    with STATE_LOCK:
        APP_STATE[mode]["logs"].append(message)


def set_running(mode: str, value: bool):
    mode = ensure_mode(mode)
    with STATE_LOCK:
        APP_STATE[mode]["running"] = value


def set_stop(mode: str, value: bool):
    mode = ensure_mode(mode)
    with STATE_LOCK:
        APP_STATE[mode]["stop"] = value


def is_stopped(mode: str) -> bool:
    mode = ensure_mode(mode)
    with STATE_LOCK:
        return APP_STATE[mode]["stop"]


def set_total(mode: str, total: int):
    mode = ensure_mode(mode)
    with STATE_LOCK:
        APP_STATE[mode]["total"] = total


def increment_total(mode: str, amount: int = 1):
    mode = ensure_mode(mode)
    with STATE_LOCK:
        APP_STATE[mode]["total"] += max(0, int(amount))


def increment_current(mode: str, amount: int = 1):
    mode = ensure_mode(mode)
    with STATE_LOCK:
        APP_STATE[mode]["current"] += max(0, int(amount))


def add_result(mode: str, row: dict):
    mode = ensure_mode(mode)
    with STATE_LOCK:
        APP_STATE[mode]["results"].append(row)
        APP_STATE[mode]["last_results"].append(row)


def add_results(mode: str, rows: list[dict]):
    mode = ensure_mode(mode)
    if not rows:
        return

    with STATE_LOCK:
        APP_STATE[mode]["results"].extend(rows)
        APP_STATE[mode]["last_results"].extend(rows)


def get_mode_state(mode: str):
    mode = ensure_mode(mode)
    with STATE_LOCK:
        results = list(APP_STATE[mode]["results"])
        last_results = list(APP_STATE[mode]["last_results"])

        # if current results become empty after finish, keep last successful data
        if not results and last_results:
            results = last_results

        from backend.result_schema import project_results

        projected_results = project_results(mode, results)

        return {
            "running": APP_STATE[mode]["running"],
            "stop": APP_STATE[mode]["stop"],
            "current": APP_STATE[mode]["current"],
            "total": APP_STATE[mode]["total"],
            "results": projected_results,
            "logs": list(APP_STATE[mode]["logs"]),
        }
