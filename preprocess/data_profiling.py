from pymongo import MongoClient
from collections import defaultdict
import pandas as pd

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['DE_training']
collection = db['glamira_url_oct2019_nov2019']

def profile_full_collection(collection):
    schema = defaultdict(lambda: defaultdict(int))
    null_counts = defaultdict(int)
    distinct_values = defaultdict(set)
    type_consistency = defaultdict(lambda: defaultdict(int))
    total_docs = 0

    cursor = collection.find()

    for doc in cursor:
        total_docs += 1
        stack = [("", doc)]

        while stack:
            prefix, subdoc = stack.pop()
            for field, value in subdoc.items():
                field_name = f"{prefix}.{field}" if prefix else field

                if isinstance(value, dict):
                    stack.append((field_name, value))
                elif isinstance(value, list):
                    schema[field_name]["list"] += 1
                    type_consistency[field_name]["list"] += 1
                    for item in value:
                        if item is None:
                            null_counts[field_name] += 1
                        elif isinstance(item, dict):
                            stack.append((field_name, item))
                        else:
                            distinct_values[field_name].add(str(item))
                else:
                    schema[field_name][type(value).__name__] += 1
                    type_consistency[field_name][type(value).__name__] += 1
                    distinct_values[field_name].add(str(value))
                    if value is None:
                        null_counts[field_name] += 1

    schema_df = pd.DataFrame(schema).fillna(0).T
    null_counts_df = pd.DataFrame(list(null_counts.items()), columns=['Field', 'Null Count']).set_index('Field')
    distinct_counts_df = pd.DataFrame([(k, len(v)) for k, v in distinct_values.items()],
                                      columns=['Field', 'Distinct Count']).set_index('Field')
    type_consistency_df = pd.DataFrame(type_consistency).fillna(0).T

    return schema_df, null_counts_df, distinct_counts_df, type_consistency_df

# Profile the full collection
schema_df, null_counts_df, distinct_counts_df, type_consistency_df = profile_full_collection(collection)

# Save individual results
schema_df.to_csv("full_schema.csv")
print("Full schema saved to full_schema.csv")
null_counts_df.to_csv("full_null_counts.csv")
print("Full null counts saved to full_null_counts.csv")
distinct_counts_df.to_csv("full_distinct_counts.csv")
print("Full distinct counts saved to full_distinct_counts.csv")
type_consistency_df.to_csv("full_type_consistency.csv")
print("Full type consistency saved to full_type_consistency.csv")

# Generate full report
full_report_df = pd.concat([
    schema_df.rename(columns=lambda x: f"Schema_{x}"),
    null_counts_df.rename(columns={'Null Count': 'Null Count'}),
    distinct_counts_df.rename(columns={'Distinct Count': 'Distinct Count'}),
    type_consistency_df.rename(columns=lambda x: f"Type_{x}")
], axis=1)

full_report_df.to_csv("full_report.csv")
full_report_df.to_csv("full_report.txt", sep='\t')
print("Full report saved to full_report.csv and full_report.txt")