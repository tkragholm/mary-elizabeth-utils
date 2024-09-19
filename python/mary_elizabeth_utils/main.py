from mary_elizabeth_utils.colored_logger import setup_colored_logger
from mary_elizabeth_utils.data_processor import DataProcessor


def main() -> None:
    logger = setup_colored_logger(__name__)

    try:
        processor = DataProcessor("python/mary_elizabeth_utils/config.yaml")
        processor.run()
        logger.info("Data processing pipeline completed successfully")
    except Exception as e:
        logger.error(f"Error in data processing pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
