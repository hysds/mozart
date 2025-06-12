# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Requirements

- Python 3.12 or higher

## Development Commands

### Installation
```bash
python setup.py develop
```

### Running the Application
```bash
# Development mode
python run.py

# Production mode
gunicorn -w2 -b 0.0.0.0:8888 -k gevent --daemon -p mozart.pid mozart:app
```

### Configuration
- Copy `settings/settings.cfg.tmpl` to `settings.cfg` and customize
- Main configuration includes Elasticsearch, RabbitMQ, Jenkins, and LDAP settings

## Architecture Overview

### Core Components

**Mozart** is a Flask-based web application that serves as the HySDS (Hybrid Science Data System) job orchestration and worker management interface. It provides both web UI and REST APIs for job management.

### Key Architectural Patterns

1. **Flask Blueprint Structure**: The application is organized into modular blueprints:
   - `mozart/views/` - Web UI views
   - `mozart/services/` - Service layer and internal APIs
   - `mozart/services/api_v01/` and `mozart/services/api_v02/` - Versioned REST APIs

2. **Elasticsearch Integration**: Heavy reliance on Elasticsearch for:
   - Job status tracking (`job_status-current` index)
   - Job specifications (`job_specs` index)
   - User rules (`user_rules-mozart` index)
   - Container information (`containers` index)

3. **Message Queue Architecture**: Uses RabbitMQ via Pika for:
   - Job orchestration and distribution
   - Queue management and monitoring
   - System-wide messaging

4. **Multi-API Versioning**: Supports both v0.1 and v0.2 REST APIs with identical endpoints but potentially different implementations

### Core Services

- **Job Management** (`mozart/services/jobs.py`, `mozart/lib/job_utils.py`): Job submission, status tracking, and lifecycle management
- **Queue Management** (`mozart/lib/queue_utils.py`): RabbitMQ queue operations and monitoring
- **Elasticsearch Services** (`mozart/services/es.py`): Direct ES operations and indexing
- **Orchestrator** (`scripts/orchestrator.py`): Background job orchestration service
- **Statistics** (`mozart/services/stats.py`): System metrics and monitoring

### Key Configuration Points

- **Reverse Proxy Support**: Built-in middleware for deployment behind nginx/Apache
- **CORS Enabled**: Currently configured for development (to be removed in production)
- **Optional Jenkins Integration**: CI/CD pipeline integration when enabled
- **SSL Context**: Configured for HTTPS deployment

### API Structure

Both API versions provide identical endpoints:
- `/job/*` - Job operations (submit, status, info)
- `/queue/*` - Queue management
- `/on-demand/*` - On-demand job handling
- `/event/*` - Event management
- `/user-rules/*` - User rule management

### Database Layer

- **SQLite**: User authentication and session management (may be replaced with SSO)
- **Elasticsearch**: Primary data store for jobs, specifications, and system state
- **RabbitMQ**: Message queue for job distribution and system communication

## Development Notes

### Job Submission Flow
1. Job specs are stored in Elasticsearch `job_specs` index
2. Jobs are submitted via API with type, queue, and parameters
3. Orchestrator picks up jobs from RabbitMQ queues
4. Job status is tracked in `job_status-current` index

### Queue System
- Queues are dynamically discovered from RabbitMQ
- Protected queues (system queues) are filtered from user-visible lists
- Job specs can specify required or recommended queues

### Swagger Documentation
API documentation is available at:
- `https://<host>/mozart/api/v0.1/` (API v0.1)
- `https://<host>/mozart/api/v0.2/` (API v0.2)