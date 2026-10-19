import psycopg2
import requests

def audit_database():
    conn = psycopg2.connect("host=127.0.0.1 port=5432 user=admin123 password=admin123 dbname=enterprise_metadata")
    cur = conn.cursor()

    print("================================================================================")
    print("                    POSTGRESQL AUDIT (enterprise_metadata)                      ")
    print("================================================================================")
    cur.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('public', 'gold_layer') AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name;
    """)
    tables = cur.fetchall()
    
    for schema, tbl in tables:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{tbl}"')
        cnt = cur.fetchone()[0]
        cur.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' AND table_name = '{tbl}' AND column_name IN ('source', 'type_method')
            ORDER BY column_name;
        """)
        cols = cur.fetchall()
        cols_str = ", ".join([f"{c[0]}({c[1]})" for c in cols]) if cols else "NONE"
        print(f"{schema}.{tbl:<33} : {cnt:>7} rows | tracking: {cols_str}")

    print("\n------------------- POSTGRES CONSTRAINTS & KEYS -------------------")
    cur.execute("""
        SELECT
            tc.table_schema, 
            tc.table_name, 
            tc.constraint_name, 
            tc.constraint_type,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema IN ('public', 'gold_layer')
          AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY', 'FOREIGN KEY')
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_type, kcu.ordinal_position;
    """)
    constraints = cur.fetchall()
    # Group by table and constraint
    grouped_c = {}
    for schema, tbl, c_name, c_type, col in constraints:
        key = (schema, tbl, c_name, c_type)
        grouped_c.setdefault(key, []).append(col)
    
    for (schema, tbl, c_name, c_type), col_list in grouped_c.items():
        if c_type in ('UNIQUE', 'FOREIGN KEY'):
            print(f"[{schema}.{tbl}] {c_type:<11} {c_name}: ({', '.join(col_list)})")

    print("\n------------------- POSTGRES DATA INTEGRITY CHECKS -------------------")
    # Check null pair_ids in tables that have pair_id
    for schema, tbl in [('gold_layer', 'crypto_klines_gold'), ('public', 'silver_crypto_klines'), ('gold_layer', 'dim_trading_pairs')]:
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = '{schema}' AND table_name = '{tbl}' AND column_name = 'pair_id';
        """)
        if cur.fetchone():
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{tbl}" WHERE pair_id IS NULL;')
            null_pairs = cur.fetchone()[0]
            print(f"{schema}.{tbl} -> NULL pair_id count: {null_pairs}")

    # Check source values distribution
    for schema, tbl in [('gold_layer', 'crypto_klines_gold'), ('public', 'silver_crypto_klines'), ('public', 'silver_trades_aggregated')]:
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = '{schema}' AND table_name = '{tbl}' AND column_name = 'source';
        """)
        if cur.fetchone():
            cur.execute(f'SELECT source, type_method, COUNT(*) FROM "{schema}"."{tbl}" GROUP BY source, type_method;')
            dist = cur.fetchall()
            print(f"{schema}.{tbl} distribution: {dist}")

    print("\n------------------- FOREIGN KEY INTEGRITY CHECK -------------------")
    cur.execute("""
        SELECT
            tc.table_schema, 
            tc.table_name, 
            kcu.column_name, 
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name 
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema IN ('public', 'gold_layer');
    """)
    fks = cur.fetchall()
    fk_violations = 0
    for s, t, col, fs, ft, fcol in fks:
        sql = f'''
            SELECT COUNT(*) FROM "{s}"."{t}" t1
            WHERE t1."{col}" IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM "{fs}"."{ft}" t2 WHERE t2."{fcol}" = t1."{col}"
            );
        '''
        try:
            cur.execute(sql)
            viol = cur.fetchone()[0]
            if viol > 0:
                print(f"[!] VIOLATION: {s}.{t}({col}) -> {fs}.{ft}({fcol}): {viol} invalid rows!")
                fk_violations += 1
            else:
                print(f"[OK] {s}.{t}({col}) -> {fs}.{ft}({fcol})")
        except Exception as e:
            print(f"[ERROR] checking {s}.{t}: {e}")
    if fk_violations == 0:
        print(">> ALL FOREIGN KEYS ARE 100% VALID AND CONSISTENT.")

    print("\n================================================================================")
    print("                         CLICKHOUSE AUDIT (default)                             ")
    print("================================================================================")
    resp = requests.post(
        "http://127.0.0.1:8123",
        params={"user": "nhfinanceadmin", "password": "nhathuy123"},
        data=b"SELECT name, engine, total_rows, sorting_key FROM system.tables WHERE database = 'default' AND is_temporary = 0 FORMAT JSON"
    )
    ch_tables = resp.json().get("data", [])
    for t in ch_tables:
        name = t.get("name")
        engine = t.get("engine")
        total_rows = t.get("total_rows")
        sorting_key = t.get("sorting_key")
        
        # Check columns
        c_resp = requests.post(
            "http://127.0.0.1:8123",
            params={"user": "nhfinanceadmin", "password": "nhathuy123"},
            data=f"SELECT name, type FROM system.columns WHERE database = 'default' AND table = '{name}' AND name IN ('source', 'type_method') FORMAT JSON".encode()
        )
        c_data = c_resp.json().get("data", [])
        ch_cols = ", ".join([f"{c['name']}({c['type']})" for c in c_data]) or "NONE"
        print(f"CH.{name:<32} | Engine: {engine:<18} | Rows: {str(total_rows):>6} | Sort: {sorting_key:<25} | Tracking: {ch_cols}")
    print("\n------------------- TIME RANGE COVERAGE -------------------")
    for s, t, col in [('gold_layer', 'crypto_klines_gold', 'open_time'), ('public', 'silver_crypto_klines', 'open_time'), ('public', 'silver_trades_aggregated', 'trade_time')]:
        cur.execute(f'SELECT MIN({col}), MAX({col}) FROM "{s}"."{t}";')
        r = cur.fetchone()
        print(f"PG {s}.{t}: {r[0]} -> {r[1]}")

    for t, col in [('crypto_klines_gold', 'open_time'), ('crypto_agg_trades_gold', 'trade_time')]:
        resp = requests.post(
            'http://127.0.0.1:8123',
            params={'user': 'nhfinanceadmin', 'password': 'nhathuy123'},
            data=f'SELECT MIN({col}), MAX({col}) FROM {t} FORMAT TabSeparated'.encode()
        )
        print(f"CH.{t}: {resp.text.strip()}")

if __name__ == "__main__":
    audit_database()
