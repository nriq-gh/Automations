import pandas as pd
import numpy as np

df_jira_users = pd.read_csv("Jira users.csv")
# List of columns to check for licenses.
products = [
    'Jira',
    'Jira Service Management',
    'Jira Work Management',
    'Jira Product Discovery',
    'Confluence',
    'Confluence Guest',
    'Bitbucket',
    'Trello',
    'Statuspage',
]

# Define the two tenants.
tenant1_tenant = 'ftenant 1'
tenant2_tenant = 'tenant2'

# Initialize dictionaries to store license counts for each tenant.
tenant1_licenses = {product: 0 for product in products}
tenant2_licenses = {product: 0 for product in products}

# Initialize a dictionary to store the number of users who have a license in both tenants.
both_tenants_licenses = {product: 0 for product in products}

# Initialize a dictionary to store the number of licenses for migration.
migration_licenses = {product: 0 for product in products}

# Iterate through each row and each product column to count licenses.
for index, row in df_jira_users.iterrows():
    for product in products:
        licenses_str = str(row[product])
        if tenant1_tenant in licenses_str:
            tenant1_licenses[product] += 1
        if tenant2_tenant in licenses_str:
            tenant2_licenses[product] += 1
        if tenant1_tenant in licenses_str and tenant2_tenant in licenses_str:
            both_tenants_licenses[product] += 1

# Calculate the licenses needed for migration (users with tenant1 license but not tenant2).
for product in products:
    migration_licenses[product] = tenant1_licenses[product] - both_tenants_licenses[product]

# Print the results in a formatted way.
print("## License Analysis per Tenant and Product\n")
print("| Product | {} Licenses | {} Licenses | Licenses in both tenants |\n".format(tenant1_tenant, tenant2_tenant))
print("|---|---|---|---|\n")
for product in products:
    print(f"| {product} | {tenant1_licenses[product]} | {tenant2_licenses[product]} | {both_tenants_licenses[product]} |\n")

print("\n------------------------------------------------\n")

print("## Licenses Required for Migration\n")
print("These are the licenses you need to migrate users from `tenant1` to `tenant2`.\n")
print("| Product | Licenses Needed |\n")
print("|---|---|\n")
for product in products:
    print(f"| {product} | {migration_licenses[product]} |\n")
