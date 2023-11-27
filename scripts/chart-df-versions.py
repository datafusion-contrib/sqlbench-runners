import pandas as pd
import matplotlib.pyplot as plt

# Reading the CSV data
data = pd.read_csv('results.csv')

# Plotting
plt.figure(figsize=(10, 6))
for version in data['version'].unique():
    version_data = data[data['version'] == version]
    plt.plot(version_data['cores'].values, version_data['duration'].values, label=f'DataFusion {version}')

plt.xlabel('Cores')
plt.ylabel('Duration (lower is better)')
plt.title('SQLBench-H @ SF10')
plt.legend()

plt.savefig('sqlbench-datafusion-versions.png')