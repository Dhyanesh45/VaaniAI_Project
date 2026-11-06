"""Step 7: check the commit rule against every interleaving.

Three actors share one log, and each step below is atomic:

  A  stale writer, epoch 1. Already committed seq 1; about to write seq 2.
  B  new owner: take over (epoch 2) -> list -> put -> check head -> ack.
  C  one compaction: read head -> list and read -> swap floor -> delete.

Breadth-first search visits every reachable state and checks one invariant in
each one: every acknowledged batch is visible to readers (it is in a segment,
or it is a log object above the floor).

Rules compared:
  put_only        a successful PUT means acknowledge (the tempting rule)
  read_after_put  PUT, then read the head; ack only if epoch matches and seq > floor

Run:  python -m archive.model_check
"""
from collections import deque
from dataclasses import dataclass, replace

MAX_SEQ = 4  # bounds retries so the state space is finite


@dataclass(frozen=True)
class State:
    epoch: int = 1
    floor: int = 0
    objects: frozenset = frozenset({(1, "A")})  # (seq, writer) in log/
    segment: frozenset = frozenset()            # (seq, writer) folded into segments
    acked: frozenset = frozenset({("A", 1)})    # (writer, seq) acknowledged
    a_pc: str = "put"
    a_seq: int = 2
    a_created: bool = False
    b_pc: str = "takeover"
    b_seq: int = 0
    b_created: bool = False
    c_pc: str = "read"
    c_epoch: int = 0
    c_floor: int = 0
    c_upto: int = 0
    c_rows: frozenset = frozenset()


def visible(s):
    above = {o for o in s.objects if o[0] > s.floor}
    return s.segment | above


def invariant(s):
    vis = visible(s)
    return all((seq, who) in vis for who, seq in s.acked)


def _put(s, seq, who):
    if any(q == seq for q, _ in s.objects):
        return s, False  # 412
    return replace(s, objects=s.objects | {(seq, who)}), True


def _writer_steps(s, who, rule, my_epoch):
    p = who.lower()
    pc, seq, created = getattr(s, f"{p}_pc"), getattr(s, f"{p}_seq"), getattr(s, f"{p}_created")

    def setw(st, **kw):
        return replace(st, **{f"{p}_{k}": v for k, v in kw.items()})

    def ack(st):
        return setw(replace(st, acked=st.acked | {(who, seq)}), pc="done")

    if pc == "put":
        s2, ok = _put(s, seq, who)
        if rule == "put_only":
            yield f"{who} PUT seq {seq} {'ok -> ack' if ok else '412 -> give up'}", (ack(s2) if ok else setw(s2, pc="done"))
        else:
            yield f"{who} PUT seq {seq} {'ok' if ok else '412'}", setw(s2, pc="check", created=ok)
    elif pc == "check":
        if s.epoch != my_epoch:
            yield f"{who} reads head: epoch {s.epoch} -> fenced", setw(s, pc="done")
        elif not created:
            yield f"{who} reads head: still owner, skip seq {seq}", setw(s, pc="put" if seq < MAX_SEQ else "done", seq=seq + 1)
        elif s.floor >= seq:
            yield f"{who} reads head: floor {s.floor} >= {seq} -> fenced", setw(s, pc="done")
        else:
            yield f"{who} reads head: ok -> ack seq {seq}", ack(s)


def steps(s, rule):
    yield from _writer_steps(s, "A", rule, 1)

    if s.b_pc == "takeover":
        yield "B takes over: epoch 1 -> 2", replace(s, epoch=s.epoch + 1, b_pc="list")
    elif s.b_pc == "list":
        top = max([q for q, _ in s.objects] + [s.floor])
        yield f"B lists log: next seq {top + 1}", replace(s, b_pc="put", b_seq=top + 1)
    elif s.b_pc in ("put", "check"):
        yield from _writer_steps(s, "B", rule, 2)

    if s.c_pc == "read":
        yield "C reads head", replace(s, c_pc="list", c_epoch=s.epoch, c_floor=s.floor)
    elif s.c_pc == "list":
        upto = max([q for q, _ in s.objects] + [s.c_floor])
        if upto <= s.c_floor:
            yield "C: nothing to compact", replace(s, c_pc="done")
        else:
            rows = frozenset(o for o in s.objects if s.c_floor < o[0] <= upto)
            yield f"C reads log up to {upto}", replace(s, c_pc="swap", c_upto=upto, c_rows=rows)
    elif s.c_pc == "swap":
        if (s.epoch, s.floor) == (s.c_epoch, s.c_floor):
            yield f"C swaps head: floor -> {s.c_upto}", replace(
                s, floor=s.c_upto, segment=s.segment | s.c_rows, c_pc="delete")
        else:
            yield "C swap fails (head moved)", replace(s, c_pc="done")
    elif s.c_pc == "delete":
        gone = frozenset(o for o in s.objects if s.c_floor < o[0] <= s.c_upto)
        yield f"C deletes log {s.c_floor + 1}..{s.c_upto}", replace(s, objects=s.objects - gone, c_pc="done")


def explore(rule):
    """Returns (number of reachable states, counterexample trace or None)."""
    start = State()
    parent = {start: None}
    queue = deque([start])
    while queue:
        s = queue.popleft()
        if not invariant(s):
            trace = []
            while parent[s] is not None:
                s, label = parent[s]
                trace.append(label)
            return len(parent), list(reversed(trace))
        for label, nxt in steps(s, rule):
            if nxt not in parent:
                parent[nxt] = (s, label)
                queue.append(nxt)
    return len(parent), None


if __name__ == "__main__":
    for rule in ("put_only", "read_after_put"):
        n, trace = explore(rule)
        print(f"{rule}: {n} states explored, {'UNSAFE' if trace else 'safe'}")
        for i, step in enumerate(trace or [], 1):
            print(f"   {i}. {step}")
