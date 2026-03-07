from prefect import task, flow, get_run_logger

@task
def extract():
    logger = get_run_logger()
    logger.info("Hello")

@flow
def main():
    extract()

if __name__ == "__main__":
    main.serve()