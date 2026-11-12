import sqlite3
from pathlib import Path

import cluster_pb2 as pb


class Storage:
    """All safety-critical state is persisted in one SQLite transaction."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (
                id INTEGER PRIMARY KEY CHECK(id=1),
                term INTEGER NOT NULL, voted_for INTEGER,
                commit_index INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO metadata VALUES (1, 0, NULL, 0);
            CREATE TABLE IF NOT EXISTS log (
                idx INTEGER PRIMARY KEY, term INTEGER NOT NULL,
                key TEXT NOT NULL, value TEXT NOT NULL, noop INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
        """)
        self.term, self.voted_for, self.commit_index = self.db.execute(
            "SELECT term, voted_for, commit_index FROM metadata WHERE id=1"
        ).fetchone()
        self.log = [
            pb.Entry(index=i, term=t, key=k, value=v, noop=bool(n))
            for i, t, k, v, n in self.db.execute(
                "SELECT idx, term, key, value, noop FROM log ORDER BY idx"
            )
        ]
        if [entry.index for entry in self.log] != list(range(1, len(self.log) + 1)):
            raise RuntimeError("Log persistido possui indices nao sequenciais.")
        if not 0 <= self.commit_index <= len(self.log):
            raise RuntimeError("Indice de commit persistido invalido.")

    def save(self) -> None:
        with self.db:
            self.db.execute(
                "UPDATE metadata SET term=?, voted_for=?, commit_index=? WHERE id=1",
                (self.term, self.voted_for, self.commit_index),
            )
            self.db.execute("DELETE FROM log")
            self.db.executemany(
                "INSERT INTO log VALUES (?, ?, ?, ?, ?)",
                [(e.index, e.term, e.key, e.value, e.noop) for e in self.log],
            )
            self.db.execute("DELETE FROM kv")
            self.db.executemany(
                "INSERT INTO kv VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [(e.key, e.value) for e in self.log[:self.commit_index] if not e.noop],
            )

    def read(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return row[0] if row is not None else None

    def term_at(self, index: int) -> int:
        return self.log[index - 1].term if index else 0

    def close(self) -> None:
        self.db.close()
