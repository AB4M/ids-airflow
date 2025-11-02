## What this pipeline does

- **Ingest (parallel)**
  - `ingest_iris`: downloads `iris.csv` (facts table).
  - `build_species_dim`: creates a small `species_dim.csv` (dimension table).
- **Transform (parallel)**
  - `transform_iris`: drops NAs, adds `sepal_ratio`, `petal_ratio`.
  - `transform_species_dim`: normalizes species text.
- **Merge & Load**
  - Joins on `species`, saves `iris_merged.csv`.
  - Loads into Postgres DB `analytics`, table `iris_features` (COPY).
- **Analysis (parallel)**
  - `analysis_plot`: reads table and saves `output/avg_petal_length.png`.
  - `analysis_model`: trains a tiny Logistic Regression and saves `output/model_result.json` (accuracy).
- **Cleanup**
  - Deletes intermediate CSVs, keeping only `iris_merged.csv` and outputs.

> Only **file paths** are passed between tasks via XCom (no DataFrames), and task groups show parallelism: `ingest`, `transform`, `analysis`.

## Repo layout

```
.
├─ docker-compose.yml
├─ Dockerfile
├─ devcontainer.json
├─ requirements.txt
├─ dags/
│  └─ iris_pipeline.py
├─ postgres-init/
│  └─ init.sql      # creates database: analytics
├─ output/          # analysis artifacts (created at runtime)
├─ logs/ plugins/   # created at runtime
└─ README.md
```
<img width="1695" height="934" alt="image" src="https://github.com/user-attachments/assets/404ebe04-bfc7-46bc-9e73-381fbeba6f19" />
<img width="1695" height="933" alt="image" src="https://github.com/user-attachments/assets/5400a6af-b321-440d-8ac6-7df5e857ca9d" />
