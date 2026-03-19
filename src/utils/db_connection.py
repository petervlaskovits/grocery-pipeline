import dotenv

env = dotenv.find_dotenv()
env_values = dotenv.dotenv_values(env)

connection_info = {
    'drivername': 'postgresql+psycopg2',
    'username': 'postgres',
    'password': env_values["POSTGRES_PWD"],
    'host': 'localhost',
    'port': 5432
}