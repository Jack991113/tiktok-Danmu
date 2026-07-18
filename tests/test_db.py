import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

import db


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = db.DB_PATH
        db.DB_PATH = os.path.join(self.temp_dir.name, "test.db")
        db.init_db()

    def tearDown(self):
        db.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_permanent_number_is_never_reused(self):
        _, first_pid = db.get_or_create_user("first", "First", "1001")
        self.assertEqual(db.delete_user("first"), first_pid)

        _, second_pid = db.get_or_create_user("second", "Second", "1002")
        self.assertGreater(second_pid, first_pid)
        self.assertEqual(db.get_user_by_identity("first", "1001")[0], first_pid)

    def test_legacy_database_migrates_without_reusing_existing_numbers(self):
        os.remove(db.DB_PATH)
        conn = sqlite3.connect(db.DB_PATH)
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, unique_id TEXT UNIQUE, display_name TEXT, permanent_id INTEGER UNIQUE)"
        )
        conn.execute("CREATE TABLE freed_pids (permanent_id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO users(unique_id, display_name, permanent_id) VALUES ('legacy', 'Legacy', 10)")
        conn.execute("INSERT INTO freed_pids(permanent_id) VALUES (12)")
        conn.commit()
        conn.close()

        db.init_db()
        _, next_pid = db.get_or_create_user("new", "New", "stable-new")
        self.assertEqual(next_pid, 13)

    def test_stable_platform_id_survives_username_change(self):
        _, permanent_id = db.get_or_create_user("old_name", "Customer", "stable-1")
        resolved = db.get_user_by_identity("new_name", "stable-1")

        self.assertEqual(resolved[0], permanent_id)
        self.assertEqual(db.get_user_by_unique_id("new_name")[0], permanent_id)

    def test_comment_events_are_ordered_and_deduplicated_by_event_id(self):
        first_id, inserted = db.record_comment_event(
            "event-1", "room", "user-a", "stable-a", "A", "88", "2026-01-01 00:00:00"
        )
        duplicate_id, duplicate_inserted = db.record_comment_event(
            "event-1", "room", "user-a", "stable-a", "A", "88", "2026-01-01 00:00:00"
        )
        second_id, second_inserted = db.record_comment_event(
            "event-2", "room", "user-b", "stable-b", "B", "88", "2026-01-01 00:00:00"
        )

        self.assertTrue(inserted)
        self.assertFalse(duplicate_inserted)
        self.assertTrue(second_inserted)
        self.assertEqual(duplicate_id, first_id)
        self.assertGreater(second_id, first_id)

    def test_many_customers_sending_same_number_are_all_persisted(self):
        def persist(index: int):
            return db.record_comment_event(
                f"event-{index}",
                "room",
                f"user-{index}",
                f"stable-{index}",
                "Customer",
                "88",
                "2026-01-01 00:00:00",
            )

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(persist, range(100)))

        ingest_ids = [event_id for event_id, inserted in results if inserted]
        self.assertEqual(len(ingest_ids), 100)
        self.assertEqual(len(set(ingest_ids)), 100)

    def test_print_jobs_follow_comment_ingest_order(self):
        event_ids = []
        for index in range(3):
            event_id, _ = db.record_comment_event(
                f"event-{index}", "room", f"user-{index}", f"stable-{index}", "User", "88", "2026-01-01 00:00:00"
            )
            event_ids.append(event_id)
            db.add_print_job(
                index + 1,
                f"user-{index}",
                "User",
                "2026-01-01 00:00:00",
                "88",
                "rendered",
                "printer",
                comment_event_id=event_id,
            )

        rows = db.fetch_pending_jobs_batch(window_seconds=5, limit=10)
        self.assertEqual([row[0] for row in rows], sorted(row[0] for row in rows))
        self.assertEqual(len(rows), len(event_ids))

    def test_crash_recovery_only_marks_started_prints_uncertain(self):
        first_job = db.add_print_job(
            1, "user-1", "User 1", "2026-01-01 00:00:00", "88", "rendered", "printer"
        )
        second_job = db.add_print_job(
            2, "user-2", "User 2", "2026-01-01 00:00:01", "88", "rendered", "printer"
        )
        claimed = db.fetch_pending_jobs_batch(window_seconds=5, limit=2)
        self.assertEqual([row[0] for row in claimed], [first_job, second_job])
        self.assertEqual(db.count_print_jobs("claimed"), 2)

        db.mark_job_processing(first_job)
        db.init_db()

        jobs = {row[0]: row for row in db.list_print_jobs()}
        self.assertEqual(jobs[first_job][6], "uncertain")
        self.assertIn("manual confirmation", jobs[first_job][11])
        self.assertEqual(jobs[second_job][6], "pending")
        self.assertEqual(db.count_print_jobs("uncertain"), 1)
        self.assertEqual(db.count_print_jobs("pending"), 1)
        summary = db.get_today_print_summary()
        self.assertEqual(summary["claimed"], 0)
        self.assertEqual(summary["processing"], 0)
        self.assertEqual(summary["uncertain"], 1)

        db.reset_job_to_pending(first_job)
        self.assertEqual(db.count_print_jobs("uncertain"), 0)
        self.assertEqual(db.count_print_jobs("pending"), 2)

    def test_database_backup_and_restore(self):
        _, original_pid = db.get_or_create_user("customer", "Customer", "stable-customer")
        backup_path = os.path.join(self.temp_dir.name, "backup.db")
        db.backup_to(backup_path)
        db.delete_user("customer")

        db.restore_from(backup_path)
        self.assertEqual(db.get_user_by_identity("customer", "stable-customer")[0], original_pid)


if __name__ == "__main__":
    unittest.main()
