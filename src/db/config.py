from decouple import config

DATABASE = {
    'user':     config("POSTGRES_USER"),
    'password': config("POSTGRES_PASSWORD"),
    'host':     config("POSTGRES_HOST"),
    'port':     5432,
    'dbname':   config("POSTGRES_DB")
}

DATABASE_RD = {
    'user':     config("POSTGRES_USER_RD"),
    'password': config("POSTGRES_PASSWORD_RD"),
    'host':     config("POSTGRES_HOST_RD"),
    'port':     config("POSTGRES_PORT_RD"),
    'dbname':   config("POSTGRES_DB_RD")
}