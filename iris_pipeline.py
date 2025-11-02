from datetime import datetime, timedelta
import os
import json
import pandas as pd
import matplotlib.pyplot as plt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from sqlalchemy import create_engine, text
from io import StringIO

# ========= 基本配置 =========
DATA_DIR = "/opt/airflow/data"
OUT_DIR = "/opt/airflow/output"
IRIS_URL = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
PG_URI_ANALYTICS = "postgresql+psycopg2://airflow:airflow@postgres:5432/analytics"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="iris_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",      # 有 schedule
    catchup=False,
    render_template_as_native_obj=True,
    description="Bare-minimum parallel ETL->Postgres->analysis DAG",
) as dag:

    # 1) 准备目录
    make_dirs = BashOperator(
        task_id="make_dirs",
        bash_command=f"mkdir -p {DATA_DIR} {OUT_DIR}"
    )

    # 2) 采集阶段（并行）
    def ingest_iris(ti):
        import requests
        iris_path = os.path.join(DATA_DIR, "iris.csv")
        r = requests.get(IRIS_URL, timeout=30)
        r.raise_for_status()
        with open(iris_path, "wb") as f:
            f.write(r.content)
        ti.xcom_push(key="iris_path", value=iris_path)

    def build_species_dim(ti):
        df = pd.DataFrame({
            "species": ["setosa", "versicolor", "virginica"],
            "species_label": [0, 1, 2],
            "is_large_petal_species": [0, 1, 1]
        })
        dim_path = os.path.join(DATA_DIR, "species_dim.csv")
        df.to_csv(dim_path, index=False)
        ti.xcom_push(key="species_dim_path", value=dim_path)

    with TaskGroup("ingest") as ingest_group:
        PythonOperator(task_id="ingest_iris", python_callable=ingest_iris)
        PythonOperator(task_id="build_species_dim", python_callable=build_species_dim)

    # 3) 转换阶段（并行）
    def transform_iris(ti):
        iris_path = ti.xcom_pull(task_ids="ingest.ingest_iris", key="iris_path")
        df = pd.read_csv(iris_path).dropna()
        df["sepal_ratio"] = df["sepal_length"] / df["sepal_width"]
        df["petal_ratio"] = df["petal_length"] / df["petal_width"]
        out_path = os.path.join(DATA_DIR, "iris_clean.csv")
        df.to_csv(out_path, index=False)
        ti.xcom_push(key="iris_clean_path", value=out_path)

    def transform_species_dim(ti):
        dim_path = ti.xcom_pull(task_ids="ingest.build_species_dim", key="species_dim_path")
        df = pd.read_csv(dim_path)
        df["species"] = df["species"].str.strip().str.lower()
        out_path = os.path.join(DATA_DIR, "species_dim_clean.csv")
        df.to_csv(out_path, index=False)
        ti.xcom_push(key="species_dim_clean_path", value=out_path)

    with TaskGroup("transform") as transform_group:
        PythonOperator(task_id="transform_iris", python_callable=transform_iris)
        PythonOperator(task_id="transform_species_dim", python_callable=transform_species_dim)

    # 4) 合并并落库（使用 psycopg2 COPY，绕开 pandas.to_sql 的判断分支）
    def merge_and_load(ti):
        iris_clean = ti.xcom_pull(task_ids="transform.transform_iris", key="iris_clean_path")
        species_clean = ti.xcom_pull(task_ids="transform.transform_species_dim", key="species_dim_clean_path")

        dfi = pd.read_csv(iris_clean)
        dfd = pd.read_csv(species_clean)
        merged = dfi.merge(dfd, on="species", how="left")

        # 导出一份合并后的 CSV（便于复查）
        os.makedirs(DATA_DIR, exist_ok=True)
        merged_path = os.path.join(DATA_DIR, "iris_merged.csv")
        merged.to_csv(merged_path, index=False)

        engine = create_engine(PG_URI_ANALYTICS)

        create_sql = """
        CREATE TABLE IF NOT EXISTS iris_features (
            sepal_length DOUBLE PRECISION,
            sepal_width DOUBLE PRECISION,
            petal_length DOUBLE PRECISION,
            petal_width DOUBLE PRECISION,
            species TEXT,
            sepal_ratio DOUBLE PRECISION,
            petal_ratio DOUBLE PRECISION,
            species_label INT,
            is_large_petal_species INT
        );
        """
        with engine.begin() as conn:
            conn.execute(text(create_sql))
            conn.execute(text("TRUNCATE TABLE iris_features;"))

        # COPY 需要原生连接
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            buf = StringIO()
            merged[[
                "sepal_length", "sepal_width", "petal_length", "petal_width",
                "species", "sepal_ratio", "petal_ratio", "species_label",
                "is_large_petal_species"
            ]].to_csv(buf, index=False, header=False)
            buf.seek(0)
            copy_sql = """
                COPY iris_features (
                    sepal_length, sepal_width, petal_length, petal_width,
                    species, sepal_ratio, petal_ratio, species_label, is_large_petal_species
                ) FROM STDIN WITH (FORMAT CSV)
            """
            cur.copy_expert(copy_sql, buf)
            raw.commit()
        finally:
            raw.close()

        ti.xcom_push(key="merged_path", value=merged_path)

    load_task = PythonOperator(task_id="merge_and_load", python_callable=merge_and_load)

    # 5) 读取数据库的小工具（避免 pandas.read_sql 触发兼容问题）
    def _read_df_via_raw():
        engine = create_engine(PG_URI_ANALYTICS)
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            cur.execute("SELECT * FROM iris_features")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=cols)
        finally:
            raw.close()

    # 6) 分析（并行）
    def analysis_plot():
        df = _read_df_via_raw()
        plt.figure()
        df.groupby("species")["petal_length"].mean().plot(kind="bar", title="Average Petal Length by Species")
        os.makedirs(OUT_DIR, exist_ok=True)
        plot_path = os.path.join(OUT_DIR, "avg_petal_length.png")
        plt.tight_layout()
        plt.savefig(plot_path)

    def analysis_model():
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score

        df = _read_df_via_raw()
        X = df[["sepal_length", "sepal_width", "petal_length", "petal_width", "sepal_ratio", "petal_ratio"]]
        y = df["species_label"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

        clf = LogisticRegression(max_iter=200)
        clf.fit(X_train, y_train)
        preds = clf.predict(X_test)
        acc = float(accuracy_score(y_test, preds))

        os.makedirs(OUT_DIR, exist_ok=True)
        with open(os.path.join(OUT_DIR, "model_result.json"), "w") as f:
            json.dump({"accuracy": acc}, f, indent=2)

    with TaskGroup("analysis") as analysis_group:
        PythonOperator(task_id="analysis_plot", python_callable=analysis_plot)
        PythonOperator(task_id="analysis_model", python_callable=analysis_model)

    # 7) 清理中间文件
    cleanup = BashOperator(
        task_id="cleanup_intermediate",
        bash_command=f'find {DATA_DIR} -type f -name "*.csv" ! -name "iris_merged.csv" -delete'
    )

    # ========= 依赖关系 =========
    make_dirs >> ingest_group
    ingest_group >> transform_group
    transform_group >> load_task
    load_task >> analysis_group
    analysis_group >> cleanup
