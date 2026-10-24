import random
import pandas as pd

random.seed(42)  # same data every run

PREFIXES = ["Apex", "Nova", "Zenith", "Orion", "Vertex", "Summit", "Pioneer",
            "Atlas", "Titan", "Helix", "Quantum", "Stellar", "Crest", "Delta", "Falcon"]
NOUNS = ["Dynamics", "Global", "Pacific", "Northern", "United", "Premier", "Allied",
         "Standard", "Prime", "Metro", "Eastern", "Western", "Union", "Century", "Frontier"]
DOMAINS = ["Logistics", "Systems", "Industries", "Holdings", "Technologies", "Foods"]
LEGAL = ["Corp", "Inc", "Ltd", "LLC"]
LEGAL_VARIANTS = {"Corp": ["Corporation", "Corp.", "Corp"],
                  "Inc": ["Incorporated", "Inc.", "Inc"],
                  "Ltd": ["Limited", "Ltd.", "Ltd"],
                  "LLC": ["L.L.C.", "LLC"]}
INDUSTRIES = {"Logistics": 180_000, "Systems": 260_000, "Industries": 220_000,
              "Holdings": 300_000, "Technologies": 350_000, "Foods": 150_000}  # revenue per employee
COUNTRIES = ["India", "Singapore", "USA", "Germany", "UK", "Japan", "Australia"]

def make_company(i):
    domain = random.choice(DOMAINS)
    name = f"{random.choice(PREFIXES)} {random.choice(NOUNS)} {domain}"
    legal = random.choice(LEGAL)
    employees = int(random.lognormvariate(5.5, 1.0)) + 10
    return {
        "id": i,
        "base_name": name,
        "legal": legal,
        "industry": domain,
        "country": random.choice(COUNTRIES),
        "employees": employees,
        "num_suppliers": max(1, int(employees ** 0.5 * random.uniform(0.5, 2))),
        "annual_revenue": round(employees * INDUSTRIES[domain] * random.uniform(0.8, 1.2)),
        "founded_year": random.randint(1960, 2020),
    }

def messy_name(c):
    """Return the same company name written slightly differently."""
    legal = random.choice(LEGAL_VARIANTS[c["legal"]])
    sep = random.choice([" ", ", "])
    name = f"{c['base_name']}{sep}{legal}"
    r = random.random()
    if r < 0.25:
        name = name.upper()
    elif r < 0.40 and len(name) > 6:      # small typo: drop one letter
        pos = random.randint(1, len(name) - 3)
        name = name[:pos] + name[pos + 1:]
    return name

# Build a master list, then unique by name
companies, seen, i = [], set(), 0
while len(companies) < 1300:
    c = make_company(i)
    if c["base_name"] not in seen:
        seen.add(c["base_name"])
        companies.append(c)
        i += 1

# Each file takes a different, overlapping sample (~1100 each, ~900 in common)
set_a = random.sample(companies, 1100)
set_b = random.sample(companies, 1100)

rows_a = [{"corporate_name": f"{c['base_name']} {c['legal']}",
           "country": c["country"], "industry": c["industry"],
           "employees": c["employees"], "num_suppliers": c["num_suppliers"],
           "_true_id": c["id"]} for c in set_a]

rows_b = [{"corporate_name": messy_name(c),
           "annual_revenue": c["annual_revenue"],
           "total_assets": round(c["annual_revenue"] * random.uniform(0.6, 1.8)),
           "founded_year": c["founded_year"],
           "_true_id": c["id"]} for c in set_b]

df_a, df_b = pd.DataFrame(rows_a), pd.DataFrame(rows_b)

# Answer key, then drop the hidden column from the real files
truth = pd.concat([
    df_a.assign(source="S1", row_id=df_a.index)[["source", "row_id", "_true_id"]],
    df_b.assign(source="S2", row_id=df_b.index)[["source", "row_id", "_true_id"]],
]).rename(columns={"_true_id": "true_company_id"})

import os
os.makedirs("data", exist_ok=True)
df_a.drop(columns="_true_id").to_csv("data/supply_chain_s1.csv", index=False)
df_b.drop(columns="_true_id").to_csv("data/financial_s2.csv", index=False)
truth.to_csv("data/ground_truth.csv", index=False)

print(len(df_a), "rows in S1;", len(df_b), "rows in S2;",
      len({c['id'] for c in set_a} & {c['id'] for c in set_b}), "companies in both")