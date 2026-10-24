import os
import re
import difflib
from pathlib import Path

import pandas as pd
import pyarrow as pa
import ray
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split

# Config comes from environment variables, so the same code runs locally and in the cluster
DATA_DIR = os.getenv("DATA_DIR", "data")            # later: gs://your-bucket/data
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "warehouse")  # later: gs://your-bucket/warehouse

LEGAL_WORDS = {"corp", "corporation", "inc", "incorporated", "ltd", "limited", "llc"}


def normalize(name: str) -> str:
    """'ACME Global Logistics, Corp.' -> 'acme global logistics'"""
    words = re.sub(r"[^a-z0-9 ]", "", name.lower().replace(",", " ")).split()
    return " ".join(w for w in words if w not in LEGAL_WORDS)


@ray.remote(num_gpus=0)
class EntityResolver:
    """Stateful actor. In production this is the LLM-backed service."""

    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY", "NOT_SET")
        if self.api_key == "NOT_SET":
            print("WARNING: LLM_API_KEY not set. Using fuzzy matching only.")

    def resolve(self, df_a: pd.DataFrame, df_b: pd.DataFrame):
        df_a, df_b = df_a.copy(), df_b.copy()
        df_a["norm"] = df_a["corporate_name"].map(normalize)
        df_b["norm"] = df_b["corporate_name"].map(normalize)

        # Every S1 company gets an ID; index them by first letter ("blocking") to keep matching fast
        df_a["canonical_id"] = range(len(df_a))
        blocks = {}
        for norm, cid in zip(df_a["norm"], df_a["canonical_id"]):
            blocks.setdefault(norm[:1], []).append((norm, cid))

        next_id = len(df_a)
        ids = []
        for norm in df_b["norm"]:
            best_id, best_score = None, 0.0
            for cand_norm, cid in blocks.get(norm[:1], []):
                score = difflib.SequenceMatcher(None, norm, cand_norm).ratio()
                if score > best_score:
                    best_id, best_score = cid, score
            if best_score >= 0.90:
                ids.append(best_id)       # same company as an S1 record
            else:
                ids.append(next_id)       # new company, only in S2
                next_id += 1
        df_b["canonical_id"] = ids
        return df_a.drop(columns="norm"), df_b.drop(columns="norm")


def train_model(df: pd.DataFrame):
    """Predict annual revenue. total_assets is left out on purpose: it is derived from revenue (data leakage)."""
    X = pd.get_dummies(
        df[["employees", "num_suppliers", "founded_year", "industry", "country"]],
        columns=["industry", "country"],
    )
    y = df["annual_revenue"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)
    model = GradientBoostingRegressor(random_state=42).fit(X_tr, y_tr)
    return r2_score(y_te, model.predict(X_te))


def write_iceberg(df: pd.DataFrame):
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.exceptions import NoSuchTableError

    wh = Path(WAREHOUSE)
    if "://" not in WAREHOUSE:
        wh.mkdir(parents=True, exist_ok=True)
        warehouse_uri, catalog_uri = wh.resolve().as_uri(), f"sqlite:///{wh.resolve()}/catalog.db"
    else:
        raise NotImplementedError("Cloud catalog is configured on Day 4")

    catalog = SqlCatalog("default", uri=catalog_uri, warehouse=warehouse_uri)
    catalog.create_namespace_if_not_exists("corporate")

    arrow = pa.Table.from_pandas(df, preserve_index=False)
    try:
        table = catalog.load_table("corporate.corporate_registry")
        result = table.upsert(arrow, join_cols=["canonical_id"])   # one atomic commit
        print(f"Upsert committed: {result.rows_inserted} inserted, {result.rows_updated} updated")
    except NoSuchTableError:
        table = catalog.create_table("corporate.corporate_registry", schema=arrow.schema)
        table.append(arrow)
        print(f"Created table and committed {len(df)} rows")


def run_pipeline():
    if not ray.is_initialized():
        # Local runs only: set LOCAL_OBJECT_STORE_MB. In the cluster KubeRay already
        # starts Ray with its own settings, so we leave it unset there.
        mb = os.getenv("LOCAL_OBJECT_STORE_MB")
        if mb:
            ray.init(object_store_memory=int(mb) * 1024 * 1024)
        else:
            ray.init()
    print("Pipeline started.")

    # 1. Ingest with Ray Data
    df_a = ray.data.read_csv(f"{DATA_DIR}/supply_chain_s1.csv").to_pandas()
    df_b = ray.data.read_csv(f"{DATA_DIR}/financial_s2.csv").to_pandas()
    print(f"Ingested {len(df_a)} rows (S1) and {len(df_b)} rows (S2)")

    # 2. Entity resolution on the actor
    resolver = EntityResolver.remote()
    res_a, res_b = ray.get(resolver.resolve.remote(df_a, df_b))

    # 3. Merge: same canonical_id means same company
    merged = res_a.merge(res_b.drop(columns="corporate_name"), on="canonical_id", how="inner")
    print(f"Matched {len(merged)} companies present in both files")

    # 4. Train
    r2 = train_model(merged)
    print(f"Model trained. R2 on held-out data: {r2:.3f}")

    # 5. Iceberg upsert
    write_iceberg(merged)
    print("Pipeline finished.")


if __name__ == "__main__":
    run_pipeline()