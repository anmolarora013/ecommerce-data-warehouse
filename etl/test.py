import os, sys
from dotenv import load_dotenv, find_dotenv

print("sys.executable:", sys.executable)

# find your .env file
env_path = find_dotenv()
print("find_dotenv() path:", env_path)

# load .env manually
load_dotenv(env_path)

print("POSTGRES_DB:", os.getenv("POSTGRES_DB"))
print("All POSTGRES_*:", {k: v for k, v in os.environ.items() if k.startswith("POSTGRES_")})
