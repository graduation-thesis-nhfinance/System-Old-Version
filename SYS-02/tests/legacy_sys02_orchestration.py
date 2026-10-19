"""
Pytest & Unittest Test Suite for SYS-02 Orchestration Components & DAG Integrity
"""

import sys
import os
import unittest
import importlib

# Ensure src/ directory is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sys02_orchestration.config import config
from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.utils.data_quality import validate_klines_data
from sys02_orchestration.utils.sla_notifier import send_telegram_alert


class TestSYS02Orchestration(unittest.TestCase):

    def test_config_port_isolation(self):
        """Verifies that SYS-02 postgres metadata DB port is isolated to 5433 and webserver to 8085."""
        self.assertEqual(config.airflow.webserver_port, 8085)
        self.assertEqual(config.airflow.postgres_host_port, 5433)
        self.assertEqual(config.lakehouse.bronze_bucket, "crypto-bronze")

    def test_lakehouse_operator_etl(self):
        """Tests Lakehouse Bronze -> Silver transformation logic."""
        op = LakehouseTaskOperator()
        res = op.process_bronze_to_silver(symbol="BTCUSDT", date_str="2026-08-07")
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["symbol"], "BTCUSDT")
        self.assertGreater(res["cleaned_ticks"], 0)
        self.assertTrue("json" in res["silver_output_path"] or "parquet" in res["silver_output_path"])


    def test_lakehouse_operator_purge(self):
        """Tests Bronze bucket purge maintenance operator."""
        op = LakehouseTaskOperator()
        res = op.clean_expired_bronze_files(retention_days=30)
        self.assertEqual(res["status"], "SUCCESS")
        self.assertGreater(res["files_purged"], 0)

    def test_clickhouse_operator(self):
        """Tests ClickHouse query execution and table freshness verification."""
        ch_op = ClickHouseTaskOperator()
        res = ch_op.verify_table_freshness("crypto_klines_gold")
        self.assertEqual(res["table_name"], "crypto_klines_gold")
        self.assertIn("is_fresh", res)

    def test_data_quality_gate_valid(self):
        """Tests data quality check gate with valid data records."""
        records = [
            {"timestamp": 100, "open": 10.0, "high": 12.0, "low": 9.5, "close": 11.0, "volume": 100.0},
            {"timestamp": 160, "open": 11.0, "high": 13.0, "low": 10.5, "close": 12.5, "volume": 50.0}
        ]
        res = validate_klines_data(records)
        self.assertTrue(res["passed"])
        self.assertEqual(res["status"], "PASSED")

    def test_data_quality_gate_invalid(self):
        """Tests data quality check gate catching null close price."""
        invalid_records = [
            {"timestamp": 100, "open": 10.0, "high": 12.0, "low": 9.5, "close": None, "volume": 100.0}
        ]
        res = validate_klines_data(invalid_records)
        self.assertFalse(res["passed"])
        self.assertEqual(res["null_close_count"], 1)

    def test_data_quality_gate_rejects_invalid_ohlc_shape(self):
        """Tests that impossible OHLC candles are blocked before Gold loading."""
        invalid_records = [
            {"timestamp": 100, "open": 10.0, "high": 9.0, "low": 9.5, "close": 11.0, "volume": 100.0}
        ]
        res = validate_klines_data(invalid_records)
        self.assertFalse(res["passed"])
        self.assertEqual(res["invalid_ohlc_count"], 1)

    def test_data_quality_gate_detects_duplicate_kline_keys(self):
        """Tests duplicate symbol/timestamp records from Binance retries or replays."""
        duplicate_records = [
            {"symbol": "BTCUSDT", "timestamp": 100, "open": 10.0, "high": 12.0, "low": 9.5, "close": 11.0, "volume": 100.0},
            {"symbol": "BTCUSDT", "timestamp": 100, "open": 10.1, "high": 12.1, "low": 9.6, "close": 11.1, "volume": 90.0},
        ]
        res = validate_klines_data(duplicate_records)
        self.assertFalse(res["passed"])
        self.assertEqual(res["duplicate_key_count"], 1)

    def test_data_quality_gate_detects_interval_gaps(self):
        """Tests missing scheduled kline intervals inside a Binance batch."""
        records = [
            {"symbol": "BTCUSDT", "timestamp": 100, "open": 10.0, "high": 12.0, "low": 9.5, "close": 11.0, "volume": 100.0},
            {"symbol": "BTCUSDT", "timestamp": 160, "open": 11.0, "high": 13.0, "low": 10.5, "close": 12.0, "volume": 100.0},
            {"symbol": "BTCUSDT", "timestamp": 340, "open": 12.0, "high": 14.0, "low": 11.5, "close": 13.0, "volume": 100.0},
        ]
        res = validate_klines_data(records)
        self.assertFalse(res["passed"])
        self.assertEqual(res["interval_gap_count"], 1)

    def test_telegram_alert_mock(self):
        """Tests notification dispatch mock functionality."""
        sent = send_telegram_alert("Test notification message", level="INFO")
        self.assertTrue(sent)

    def test_dag_import_01_binance_klines_etl(self):
        """Verifies syntax and structure of DAG 01."""
        dag_mod = importlib.import_module("dags.01_binance_klines_etl_dag")
        self.assertTrue(hasattr(dag_mod, 'dag'))
        self.assertEqual(dag_mod.dag.dag_id, '01_binance_klines_etl_dag')

    def test_dag_import_02_ml_model_retrain(self):
        """Verifies syntax and structure of DAG 02."""
        dag_mod = importlib.import_module("dags.02_ml_model_retrain_dag")
        self.assertTrue(hasattr(dag_mod, 'dag'))
        self.assertEqual(dag_mod.dag.dag_id, '02_ml_model_retrain_dag')

    def test_dag_import_03_lakehouse_maintenance(self):
        """Verifies syntax and structure of DAG 03."""
        dag_mod = importlib.import_module("dags.03_lakehouse_maintenance_dag")
        self.assertTrue(hasattr(dag_mod, 'dag'))
        self.assertEqual(dag_mod.dag.dag_id, '03_lakehouse_maintenance_dag')


if __name__ == "__main__":
    unittest.main()



