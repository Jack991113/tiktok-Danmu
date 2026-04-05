import sqlite3
import threading
import os
import shutil
from typing import Optional, Tuple, List


def _default_db_path() -> str:
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
        root = os.path.join(base, "SenNails")
    else:
        root = os.path.join(os.path.expanduser("~"), ".sen_nails")
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, "data.db")
    # One-time migration from legacy relative DB (project root / startup cwd).
    legacy = os.path.join(os.getcwd(), "data.db")
    if (not os.path.exists(path)) and os.path.exists(legacy):
        try:
            shutil.copy2(legacy, path)
        except Exception:
            pass
    return path


DB_PATH = _default_db_path()
_lock = threading.Lock()


def init_db() -> None:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            unique_id TEXT UNIQUE,
            display_name TEXT,
            permanent_id INTEGER UNIQUE
        )
        """
        )
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS freed_pids (
            permanent_id INTEGER PRIMARY KEY
        )
        """
        )
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS blacklist (
            id INTEGER PRIMARY KEY,
            unique_id TEXT UNIQUE
        )
        """
        )
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS print_jobs (
            id INTEGER PRIMARY KEY,
            permanent_id INTEGER,
            unique_id TEXT,
            display_name TEXT,
            time TEXT,
            content TEXT,
            raw_message TEXT DEFAULT '',
            rule_hit TEXT DEFAULT '',
            rendered TEXT,
            printer TEXT,
            printer_size TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            fail_reason TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )
        conn.commit()
        # Ensure printer_size column exists for older DBs
        cur.execute("PRAGMA table_info(print_jobs)")
        cols = [r[1] for r in cur.fetchall()]
        if 'printer_size' not in cols:
            cur.execute("ALTER TABLE print_jobs ADD COLUMN printer_size TEXT DEFAULT ''")
            conn.commit()
        if 'raw_message' not in cols:
            cur.execute("ALTER TABLE print_jobs ADD COLUMN raw_message TEXT DEFAULT ''")
            conn.commit()
        if 'rule_hit' not in cols:
            cur.execute("ALTER TABLE print_jobs ADD COLUMN rule_hit TEXT DEFAULT ''")
            conn.commit()
        if 'fail_reason' not in cols:
            cur.execute("ALTER TABLE print_jobs ADD COLUMN fail_reason TEXT DEFAULT ''")
            conn.commit()
        if 'created_at' not in cols:
            # Legacy DB compatibility: dispatcher batches rely on created_at.
            cur.execute("ALTER TABLE print_jobs ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            conn.commit()
        if 'printed_at' not in cols:
            # For reconnect dedupe we need real print-success timestamp.
            cur.execute("ALTER TABLE print_jobs ADD COLUMN printed_at TIMESTAMP")
            conn.commit()
        if 'trace_id' not in cols:
            cur.execute("ALTER TABLE print_jobs ADD COLUMN trace_id TEXT DEFAULT ''")
            conn.commit()
        # Speed up reconnect dedupe query on large history.
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_print_jobs_uid_msg_status_time "
            "ON print_jobs(unique_id, raw_message, status, printed_at, created_at)"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_print_jobs_trace_id ON print_jobs(trace_id)")
        conn.commit()
        # Crash-safe recovery: move stale/in-flight jobs back to pending on startup.
        cur.execute("UPDATE print_jobs SET status = 'pending' WHERE status = 'processing'")
        conn.commit()
        conn.close()


def get_or_create_user(unique_id: str, display_name: Optional[str] = None) -> Tuple[int, int]:
    """Return (db_id, permanent_id). Create user if missing."""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT id, permanent_id FROM users WHERE unique_id = ?", (unique_id,))
        row = cur.fetchone()
        if row:
            conn.close()
            return row[0], row[1]

        # prefer reusing a freed permanent_id
        cur.execute("SELECT permanent_id FROM freed_pids ORDER BY permanent_id LIMIT 1")
        freed = cur.fetchone()
        if freed:
            next_pid = int(freed[0])
            cur.execute("DELETE FROM freed_pids WHERE permanent_id = ?", (next_pid,))
        else:
            # compute next permanent_id
            cur.execute("SELECT MAX(permanent_id) FROM users")
            m = cur.fetchone()[0] or 0
            next_pid = m + 1

        cur.execute("INSERT INTO users (unique_id, display_name, permanent_id) VALUES (?, ?, ?)", (unique_id, display_name or "", next_pid))
        conn.commit()
        db_id = cur.lastrowid
        conn.close()
        return db_id, next_pid


def get_user_by_unique_id(unique_id: str) -> Optional[Tuple[int, str]]:
    """Return (permanent_id, display_name) for an existing user, or None."""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT permanent_id, display_name FROM users WHERE unique_id = ?", (unique_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return int(row[0]), (row[1] or "")


def upsert_user_fixed_permanent_id(unique_id: str, display_name: str, permanent_id: int) -> Tuple[bool, str]:
    uid = str(unique_id or "").strip()
    name = str(display_name or "").strip()
    try:
        pid = int(permanent_id)
    except Exception:
        return False, "invalid_permanent_id"
    if not uid or pid <= 0:
        return False, "missing_unique_id_or_pid"
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT permanent_id FROM users WHERE unique_id = ?", (uid,))
        row = cur.fetchone()
        if row:
            current_pid = int(row[0] or 0)
            if current_pid != pid:
                conn.close()
                return False, f"unique_id_conflict:{current_pid}"
            cur.execute("UPDATE users SET display_name = ? WHERE unique_id = ?", (name, uid))
            conn.commit()
            conn.close()
            return True, "updated"
        cur.execute("SELECT unique_id FROM users WHERE permanent_id = ?", (pid,))
        taken = cur.fetchone()
        if taken and str(taken[0] or "").strip() != uid:
            conn.close()
            return False, f"permanent_id_conflict:{taken[0]}"
        cur.execute(
            "INSERT INTO users (unique_id, display_name, permanent_id) VALUES (?, ?, ?)",
            (uid, name, pid),
        )
        conn.commit()
        conn.close()
        return True, "inserted"


def is_blacklisted(unique_id: str) -> bool:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM blacklist WHERE unique_id = ?", (unique_id,))
        exists = cur.fetchone() is not None
        conn.close()
        return exists


def add_blacklist(unique_id: str) -> None:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO blacklist (unique_id) VALUES (?)", (unique_id,))
        conn.commit()
        conn.close()


def remove_blacklist(unique_id: str) -> None:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM blacklist WHERE unique_id = ?", (unique_id,))
        conn.commit()
        conn.close()


def list_blacklist() -> List[str]:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT unique_id FROM blacklist ORDER BY id")
        rows = [r[0] for r in cur.fetchall()]
        conn.close()
        return rows


def list_users() -> List[Tuple[str, Optional[str], int]]:
    """Return list of users as tuples (unique_id, display_name, permanent_id)."""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT unique_id, display_name, permanent_id FROM users ORDER BY permanent_id")
        rows = cur.fetchall()
        conn.close()
        return rows


def list_users_dicts() -> List[dict]:
    return [
        {"unique_id": str(unique_id or ""), "display_name": str(display_name or ""), "permanent_id": int(permanent_id or 0)}
        for unique_id, display_name, permanent_id in list_users()
    ]


def delete_user(unique_id: str) -> Optional[int]:
    """Delete user by unique_id and free their permanent_id for reuse. Returns freed permanent_id or None."""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT permanent_id FROM users WHERE unique_id = ?", (unique_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return None
        pid = int(row[0])
        cur.execute("DELETE FROM users WHERE unique_id = ?", (unique_id,))
        # Add to freed pool
        cur.execute("INSERT OR IGNORE INTO freed_pids (permanent_id) VALUES (?)", (pid,))
        conn.commit()
        conn.close()
        return pid


def delete_all_users() -> int:
    """Delete all user mappings and clear freed pool. Returns deleted count."""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        count = int(cur.fetchone()[0] or 0)
        cur.execute("DELETE FROM users")
        cur.execute("DELETE FROM freed_pids")
        conn.commit()
        conn.close()
        return count


def blacklist_and_remove(unique_id: str) -> Tuple[bool, Optional[int]]:
    """Add user to blacklist and remove their user mapping (free ID). Returns (blacklisted, freed_pid)."""
    add_blacklist(unique_id)
    freed = delete_user(unique_id)
    return True, freed


def add_print_job(
    permanent_id: int,
    unique_id: str,
    display_name: str,
    when: str,
    content: str,
    rendered: str,
    printer: str,
    printer_size: str = '',
    raw_message: str = '',
    rule_hit: str = '',
    trace_id: str = '',
) -> int:
    """Insert a print job into DB and return job id."""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO print_jobs (permanent_id, unique_id, display_name, time, content, raw_message, rule_hit, rendered, printer, printer_size, trace_id, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
            (permanent_id, unique_id, display_name or '', when, content or '', raw_message or '', rule_hit or '', rendered or '', printer or '', printer_size or '', trace_id or ''),
        )
        conn.commit()
        jid = cur.lastrowid
        conn.close()
        return jid


def fetch_next_pending_job() -> Optional[Tuple[int, int, str, str, str, str, str, str, str]]:
    """Fetch the oldest pending job and mark it as 'processing'. Returns the row or None.
    Row format: (id, permanent_id, unique_id, display_name, time, content, rendered, printer)"""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT id, permanent_id, unique_id, display_name, time, content, rendered, printer, printer_size FROM print_jobs WHERE status = 'pending' ORDER BY id LIMIT 1")
        row = cur.fetchone()
        if not row:
            conn.close()
            return None
        jid = row[0]
        cur.execute("UPDATE print_jobs SET status = 'processing' WHERE id = ?", (jid,))
        conn.commit()
        conn.close()
        return row


def fetch_pending_jobs_batch(window_seconds: int = 5, limit: int = 50) -> List[Tuple[int, int, str, str, str, str, str, str, str, str]]:
    """Fetch a pending batch by created_at time window and mark them as processing.

    Strategy:
    1. Find the oldest pending job's created_at.
    2. Claim jobs whose created_at falls within [oldest, oldest + window_seconds], up to limit.
    3. Mark claimed jobs as processing atomically under DB lock.
    """
    win = max(1, int(window_seconds))
    lim = max(1, int(limit))
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        try:
            cur.execute("SELECT created_at FROM print_jobs WHERE status = 'pending' ORDER BY id LIMIT 1")
            first = cur.fetchone()
            if not first:
                conn.close()
                return []
            anchor_created_at = first[0]
            modifier = f"+{win} seconds"
            cur.execute(
                """
                SELECT id, permanent_id, unique_id, display_name, time, content, rendered, printer, printer_size
                     , trace_id
                FROM print_jobs
                WHERE status = 'pending'
                  AND created_at <= datetime(?, ?)
                ORDER BY id
                LIMIT ?
                """,
                (anchor_created_at, modifier, lim),
            )
            rows = cur.fetchall()
        except sqlite3.OperationalError:
            # Compatibility fallback when legacy DB misses created_at.
            cur.execute(
                """
                SELECT id, permanent_id, unique_id, display_name, time, content, rendered, printer, printer_size
                     , trace_id
                FROM print_jobs
                WHERE status = 'pending'
                ORDER BY id
                LIMIT ?
                """,
                (lim,),
            )
            rows = cur.fetchall()
        if not rows:
            conn.close()
            return []
        ids = [r[0] for r in rows]
        placeholders = ",".join("?" for _ in ids)
        cur.execute(f"UPDATE print_jobs SET status = 'processing' WHERE id IN ({placeholders})", ids)
        conn.commit()
        conn.close()
        return rows


def mark_job_printed(job_id: int) -> None:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "UPDATE print_jobs SET status = 'printed', fail_reason = '', printed_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )
        conn.commit()
        conn.close()


def mark_job_failed(job_id: int, reason: Optional[str] = None) -> None:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE print_jobs SET status = 'failed', fail_reason = ? WHERE id = ?", ((reason or '')[:500], job_id))
        conn.commit()
        conn.close()


def list_print_jobs(status: Optional[str] = None) -> List[Tuple]:
    """Return list of print jobs. If status provided, filter by status."""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        if status:
            cur.execute("SELECT id, permanent_id, unique_id, display_name, time, content, status, printer, printer_size, created_at, rule_hit, fail_reason, trace_id FROM print_jobs WHERE status = ? ORDER BY id", (status,))
        else:
            cur.execute("SELECT id, permanent_id, unique_id, display_name, time, content, status, printer, printer_size, created_at, rule_hit, fail_reason, trace_id FROM print_jobs ORDER BY id")
        rows = cur.fetchall()
        conn.close()
        return rows


def run_maintenance() -> dict:
    """Run lightweight DB maintenance and return summary."""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        out = {"vacuum": False, "analyze": False, "error": ""}
        try:
            cur.execute("PRAGMA optimize")
            cur.execute("ANALYZE")
            out["analyze"] = True
            conn.commit()
            # VACUUM must be outside transaction in sqlite.
            cur.execute("VACUUM")
            out["vacuum"] = True
        except Exception as e:
            out["error"] = str(e)
        finally:
            conn.close()
        return out


def get_today_print_summary() -> dict:
    """Return today's print summary and top failure reasons."""
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status='processing' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status='printed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END)
            FROM print_jobs
            WHERE date(created_at, 'localtime') = date('now', 'localtime')
            """
        )
        p, pr, ok, f = cur.fetchone() or (0, 0, 0, 0)
        cur.execute(
            """
            SELECT fail_reason, COUNT(*)
            FROM print_jobs
            WHERE status='failed'
              AND date(created_at, 'localtime') = date('now', 'localtime')
            GROUP BY fail_reason
            ORDER BY COUNT(*) DESC
            LIMIT 10
            """
        )
        reasons = cur.fetchall()
        conn.close()
        return {
            'pending': int(p or 0),
            'processing': int(pr or 0),
            'printed': int(ok or 0),
            'failed': int(f or 0),
            'top_fail_reasons': reasons,
        }


def get_recent_failed_count(seconds: int = 60) -> int:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM print_jobs
            WHERE status='failed'
              AND created_at >= datetime('now', ?)
            """,
            (f"-{max(1, int(seconds))} seconds",),
        )
        v = cur.fetchone()[0] or 0
        conn.close()
        return int(v)


def has_recent_printed(unique_id: str, raw_message: str, seconds: int = 20) -> bool:
    """Whether the same user's same message was printed successfully in recent seconds."""
    uid = str(unique_id or "").strip()
    msg = str(raw_message or "").strip()
    if not uid or not msg:
        return False
    win = max(1, int(seconds))
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM print_jobs
            WHERE status = 'printed'
              AND unique_id = ?
              AND raw_message = ?
              AND COALESCE(printed_at, created_at) >= datetime('now', ?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (uid, msg, f"-{win} seconds"),
        )
        row = cur.fetchone()
        conn.close()
        return row is not None


def has_recent_job_duplicate(unique_id: str, raw_message: str, seconds: int = 20) -> bool:
    """Whether same user's same message exists in recent pending/processing/printed jobs."""
    uid = str(unique_id or "").strip()
    msg = str(raw_message or "").strip()
    if not uid or not msg:
        return False
    win = max(1, int(seconds))
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM print_jobs
            WHERE unique_id = ?
              AND raw_message = ?
              AND status IN ('pending', 'processing', 'printed')
              AND (
                    CASE
                      WHEN status = 'printed' THEN COALESCE(printed_at, created_at)
                      ELSE created_at
                    END
                  ) >= datetime('now', ?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (uid, msg, f"-{win} seconds"),
        )
        row = cur.fetchone()
        conn.close()
        return row is not None


def reset_job_to_pending(job_id: int) -> None:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE print_jobs SET status = 'pending' WHERE id = ?", (job_id,))
        conn.commit()
        conn.close()


def delete_print_job(job_id: int) -> None:
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM print_jobs WHERE id = ?", (job_id,))
        conn.commit()
        conn.close()


def delete_print_jobs_by_status(status_list: List[str]) -> int:
    """Delete print jobs by status list and return deleted count."""
    statuses = [s for s in status_list if s]
    if not statuses:
        return 0
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in statuses)
        cur.execute(f"DELETE FROM print_jobs WHERE status IN ({placeholders})", statuses)
        deleted = cur.rowcount or 0
        conn.commit()
        conn.close()
        return int(deleted)

def import_permanent_ids(data_list: List[dict]) -> dict:
    """
    Import permanent IDs from external software (CSV format).
    Expected dict keys: permanent_id, unique_id, display_name
    Returns: {imported: count, skipped: count, conflicts: list}
    """
    with _lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        imported = 0
        skipped = 0
        conflicts = []
        
        # Get current max permanent_id for auto-resolution
        cur.execute("SELECT MAX(permanent_id) FROM users")
        max_pid = (cur.fetchone()[0] or 0)
        
        for row in data_list:
            try:
                perm_id = int(row.get('permanent_id', 0))
                unique_id = str(row.get('unique_id', '')).strip()
                display_name = str(row.get('display_name', '')).strip()
                
                if not unique_id or perm_id <= 0:
                    skipped += 1
                    continue
                
                # Check if user exists
                cur.execute("SELECT id, permanent_id FROM users WHERE unique_id = ?", (unique_id,))
                existing = cur.fetchone()
                
                if existing:
                    # User already exists, skip or update
                    existing_id, existing_perm = existing
                    if existing_perm != perm_id:
                        conflicts.append(f"{unique_id}: 已存在ID {existing_perm}，导入ID {perm_id} 冲突")
                    skipped += 1
                    continue
                
                # Check if permanent_id is already taken
                cur.execute("SELECT unique_id FROM users WHERE permanent_id = ?", (perm_id,))
                if cur.fetchone():
                    # Auto-resolve: use new ID
                    max_pid += 1
                    use_perm_id = max_pid
                    conflicts.append(f"{unique_id}: 永久ID {perm_id} 已被占用，自动分配 {use_perm_id}")
                else:
                    use_perm_id = perm_id
                
                # Insert new user
                cur.execute(
                    "INSERT INTO users (unique_id, display_name, permanent_id) VALUES (?, ?, ?)",
                    (unique_id, display_name, use_perm_id)
                )
                imported += 1
            except Exception as e:
                skipped += 1
                conflicts.append(f"行错误: {e}")
        
        conn.commit()
        conn.close()
        
        return {
            'imported': imported,
            'skipped': skipped,
            'conflicts': conflicts
        }
