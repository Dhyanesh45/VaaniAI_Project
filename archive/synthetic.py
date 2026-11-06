"""Synthetic stations for tests and the demo. Three behaviours:

  coastal  mild, noisy daily cycle; humidity moves OPPOSITE to temperature
  desert   strong, clean daily cycle; humidity moves WITH temperature (shifted)
  flaky    no rhythm, mostly noise, long reporting gaps

Coastal and desert differ in level by about 10 degrees, but that is not what
separates them. Fingerprints normalise level away, so they are told apart by
how clean the cycle is and how the channels relate to each other.
"""
import math
import random

DAY = 86400


def station_points(name, kind, start, days, cadence_s=1800, seed=0):
    rng = random.Random(f"{name}-{seed}")
    points = []
    pressure = 1013.0
    t = start
    silent_until = 0
    while t < start + days * DAY:
        phase = 2 * math.pi * ((t % DAY) / DAY)
        pressure += rng.gauss(0, 0.3)
        if kind == "flaky":
            if t < silent_until:
                t += cadence_s
                continue
            if rng.random() < 0.01:
                silent_until = t + rng.randint(6, 48) * 3600
            temp = 18 + rng.gauss(0, 4)
            hum = 50 + rng.gauss(0, 15)
        elif kind == "coastal":
            temp = 15 + 3 * math.sin(phase) + rng.gauss(0, 1.5)
            hum = 75 - 8 * math.sin(phase) + rng.gauss(0, 4)
        elif kind == "desert":
            temp = 25 + 12 * math.sin(phase) + rng.gauss(0, 0.5)
            hum = 20 + 6 * math.sin(phase - 0.5) + rng.gauss(0, 0.5)
        else:
            raise ValueError(kind)
        for channel, v in (("temp", temp), ("humidity", hum), ("pressure", pressure)):
            points.append({"station": name, "channel": channel, "t": t, "v": round(v, 3), "rev": 0})
        t += cadence_s
    return points
