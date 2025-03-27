# Credentials Setup

1. Copy `sample_credentials.json` to `credentials.json`
2. Fill in your actual credentials
3. Never commit actual credentials to git

The sample credentials file assumes simple username/password access for most vendors.
Depending on how your organization manages programmatic access to a given vendor and 
if there are any restrictions in place, you may need to change the layout of the
credentials file and modify code to ensure it is correctly read by the client you
intend on using.