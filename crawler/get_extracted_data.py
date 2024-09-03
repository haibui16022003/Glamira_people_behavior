import json

input_file = 'products_data.json'
output_file = 'extracted_data.json'

with open(input_file, 'r') as f:
    data = f.read()
    json_objects = data.splitlines()

with open(output_file, 'w') as outfile:
    for line in json_objects:
        try:
            data = json.loads(line.strip())
            extracted_data = data.get('extracted_data', [])
            for entry in extracted_data:
                outfile.write(json.dumps(entry) + '\n')
        except json.JSONDecodeError as e:
            print(f"Skipping line due to JSONDecodeError: {e}")

print(f"Extracted data has been saved to {output_file}")
