"""Small shared helpers for synthetic, independently seeded demand patterns."""

from datetime import timedelta
from zoneinfo import ZoneInfo

import numpy as np

from ..model import Intent


def opaque_id(prefix, rng):
    return f"{prefix}-{rng.bytes(12).hex()}"


def live(file, at):
    return file.created_at <= at and (file.deleted_at is None or at < file.deleted_at)


def working_factor(actor, at, holiday_activity):
    local = at.astimezone(ZoneInfo(actor.timezone))
    hhmm = local.strftime("%H:%M")
    if (
        local.isoweekday() not in actor.work_days
        or not actor.schedule_start_local <= hhmm < actor.schedule_end_local
        or any(start <= at < end for start, end in actor.vacations)
    ):
        return 0.0
    return holiday_activity if local.date() in actor.holidays else 1.0


def hours(volume, window):
    origin = volume.period_start.replace(minute=0, second=0, microsecond=0)
    start, end = max(volume.period_start, window[0]), min(volume.period_end, window[1])
    index = max(0, int((start - origin).total_seconds() // 3600))
    hour = origin + timedelta(hours=index)
    while hour < end:
        yield index, hour
        index += 1
        hour += timedelta(hours=1)


def hour_rng(seed, volume_index, hour_index):
    return np.random.default_rng(np.random.SeedSequence([seed, volume_index, hour_index]))


def timestamp(hour, rng, jitter):
    # Jitter never escapes its canonical hour, including jitter_scale > 1.
    offset = np.clip(0.5 + (rng.random() - 0.5) * jitter, 0, 1 - 1e-9)
    return hour + timedelta(seconds=float(offset) * 3600)


def intent(at, actor, file, cause, rng, operation="open"):
    apps = tuple(actor.app_weights) or ("office",)
    weights = list(actor.app_weights.values()) or [1.0]
    app = str(rng.choice(apps, p=np.asarray(weights) / sum(weights)))
    return Intent(
        opaque_id("int", rng),
        at,
        file.volume_id,
        actor.actor_id,
        file.file_id,
        app,
        file.read_profile,
        1048576 if file.read_profile == "head" else None,
        cause,
        operation=operation,
    )


def in_window(at, volume, window):
    return max(volume.period_start, window[0]) <= at < min(volume.period_end, window[1])
