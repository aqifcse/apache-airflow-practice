# Apache Airflow Practice Project

This project demonstrates the use of Apache Airflow 2.7.1 for orchestrating data pipelines. It includes sample Directed Acyclic Graphs (DAGs) that showcase common data-pipeline patterns, scheduling, dependencies, and task composition.

## Project Structure

```
apache-airflow-practice/
├── dags/
│   ├── etl_pipeline.py       # Basic ETL pipeline example
│   └── complex_pipeline.py         # Complex pipeline with DB storage
├── plugins/
│   └── custom_operators/
│       └── __init__.py       # Custom operators initialization
├── logs/                     # Airflow logs directory
├── docker-compose.yml        # Docker configuration
├── requirements.txt          # Python dependencies
├── .gitignore               # Git ignore rules
└── README.md                # Project documentation
```

## Prerequisites

- Docker Engine
- Docker Compose
- Git

## Quick Start

1. **Clone the repository**:
```bash
git clone <repository-url>
cd apache-airflow-practice
```

2. **Create required directories**:
```bash
mkdir -p ./dags ./plugins ./logs
```

3. **Start Airflow**:
```bash
docker-compose up -d
```

4. **Access Airflow UI**:
- URL: http://localhost:8080
- Username: admin
- Password: admin

## DAG Details

### ETL Pipeline (etl_pipeline.py)
A basic ETL workflow demonstrating:
- Data extraction from source
- Data transformation using Python
- Data loading with error handling
- XCom usage for task communication

### Complex Pipeline (complex_pipeline.py)
Advanced pipeline showcasing:
- PostgreSQL database integration
- Temporary data storage
- Task dependencies
- Custom operator usage

## Configuration

The project uses Docker Compose with:
- Apache Airflow 2.7.1
- PostgreSQL 13
- LocalExecutor configuration
- Volume mounts for dags, plugins, and logs

## Development

To add new DAGs:
1. Create Python files in the `dags/` directory
2. Follow Airflow's DAG specification
3. Ensure proper task dependencies
4. Test locally before deployment

## Troubleshooting

Common issues and solutions:
- Database initialization: Run `docker-compose down -v` and restart
- Permission issues: Check volume mount permissions
- Connection errors: Verify PostgreSQL connection settings

## License

This project is licensed under the MIT License - see the LICENSE file for details.