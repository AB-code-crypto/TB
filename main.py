import asyncio
import os

from t_tech.invest import AsyncClient
from dotenv import load_dotenv
load_dotenv()
TOKEN = os.environ["INVEST_TOKEN"]


async def main():
    async with AsyncClient(TOKEN) as client:
        print(await client.users.get_accounts())


if __name__ == "__main__":
    asyncio.run(main())
