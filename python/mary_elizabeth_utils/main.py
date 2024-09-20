import mary_elizabeth_utils as meu


def main() -> None:
    logger = meu.setup_colored_logger(__name__)

    try:
        processor = meu.DataProcessor("python/mary_elizabeth_utils/config.yaml")
        processor.run()
        logger.info("Data processing pipeline completed successfully")
    except Exception as e:
        logger.error(f"Error in data processing pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
