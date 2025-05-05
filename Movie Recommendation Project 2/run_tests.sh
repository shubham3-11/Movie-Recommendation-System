#!/bin/bash

# Colors for output formatting
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Display help information
function show_help {
    echo -e "${BLUE}Movie Recommendation System Test Runner${NC}"
    echo "Usage: ./run_tests.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --install        Install test dependencies"
    echo "  --all            Run all tests"
    echo "  --data           Run data processing tests"
    echo "  --model          Run model tests"
    echo "  --backend        Run backend API tests"
    echo "  --coverage       Generate coverage report"
    echo "  --help           Show this help message"
    echo ""
}

# Install dependencies
function install_dependencies {
    echo -e "${BLUE}Installing test dependencies...${NC}"
    pip install -r requirements.txt
    pip install pytest pytest-cov
    echo -e "${GREEN}Dependencies installed successfully!${NC}"
}

# Run all tests
function run_all_tests {
    echo -e "${BLUE}Running all tests...${NC}"
    python -m pytest tests/data_process_test.py tests/test_kafka.py tests/test_svd_model.py tests/test_model_app.py tests/test_backend_app.py tests/test_config.py -v
}

# Run data quality tests
function run_data_tests {
    echo -e "${BLUE}Running data quality tests...${NC}"
    python -m pytest tests/data_process_test.py tests/test_kafka.py -v
}

# Run model tests
function run_model_tests {
    echo -e "${BLUE}Running model tests...${NC}"
    python -m pytest tests/test_svd_model.py tests/test_model_app.py -v
}

# Run backend tests
function run_backend_tests {
    echo -e "${BLUE}Running backend API tests...${NC}"
    python -m pytest tests/test_backend_app.py -v
}

# Generate coverage report
function generate_coverage {
    echo -e "${BLUE}Generating coverage report...${NC}"
    python -m pytest tests/data_process_test.py tests/test_kafka.py tests/test_svd_model.py tests/test_model_app.py tests/test_backend_app.py tests/test_config.py --cov=data_processing --cov=inference_service --cov=model_training --cov-report=term --cov-report=html
    echo -e "${GREEN}Coverage report generated! See htmlcov/index.html for details.${NC}"
}

# Main script execution
if [ $# -eq 0 ]; then
    run_all_tests
    exit 0
fi

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help)
            show_help
            exit 0
            ;;
        --install)
            install_dependencies
            shift
            ;;
        --all)
            run_all_tests
            shift
            ;;
        --data)
            run_data_tests
            shift
            ;;
        --model)
            run_model_tests
            shift
            ;;
        --backend)
            run_backend_tests
            shift
            ;;
        --coverage)
            generate_coverage
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

echo -e "${GREEN}Tests completed!${NC}"