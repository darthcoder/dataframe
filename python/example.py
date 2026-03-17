"""
Example: Polars -> Haskell -> Polars round-trip via Arrow C Data Interface.
Run from repo root:
    python3 -m venv
    source ./venv/bin/activate
    pip install polars
    cabal build dataframe-arrow
    python3 python/example.py
"""
import polars as pl
import hyrax as hx

raw = pl.read_csv("data/titanic.csv")
print("Polars input shape:", raw.shape)

result = (hx.from_arrow(raw.to_arrow())
            .groupBy(["Sex"])
            .aggregate({
                "survived_sum": hx.sum(hx.col("Survived")),
                "n":            hx.count(hx.col("Sex")),
            }))

print("\nGroupBy result:")
print(pl.from_arrow(result))
