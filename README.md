# AIOps Monitoring and Event Processing

This project is a lightweight AIOps simulation for the `payment-service`. It monitors
service telemetry, identifies abnormal behaviour, creates anomaly events, and moves
those events through an in-memory producer, topic, and consumer workflow.

## Scenario

The operational problem is detecting payment-service degradation early. Slow responses,
high resource usage, and error logs can indicate timeouts or database connectivity
problems. AIOps combines these signals into actionable anomaly events so that an
operations team can inspect one consistent result instead of reviewing raw records only.

## Repository Components

- `data/service_data.json` contains the synthetic service telemetry.
- `src/anomaly_detector.py` applies thresholds and checks log levels.
- `src/event_producer.py` publishes detected events.
- `src/event_topic.py` provides the in-memory event topic.
- `src/event_consumer.py` reads events from the topic.
- `src/aiops_pipeline.py` loads the data and runs the end-to-end workflow.
- `tests/` validates detection and event delivery behaviour.

## Operational Data Analysis

Each record has an ISO-style timestamp at one-minute intervals from `10:00` through
`10:09` on 20 September 2026. The timestamp provides event ordering and identifies
when the service behaviour changed.

The metric fields are `response_time_ms`, `cpu_percent`, and `memory_percent`. The log
fields are `log_level` and `message`. `service` identifies the monitored service.

Records from `10:00` through `10:04` and `10:07` through `10:09` represent normal
behaviour: response times are 120-150 ms, CPU is 42-50%, memory is 51-57%, and the log
level is `INFO`.

The records at `10:05` and `10:06` are anomalous:

| Time | Metrics and log evidence | Detection reasons |
| --- | --- | --- |
| `10:05` | 610 ms response time, `ERROR`, payment service timeout | High response time; error log |
| `10:06` | 640 ms response time, 94% CPU, 91% memory, `ERROR`, database connection timeout | High response time; high CPU; high memory; error log |

## Detection Findings

The detector uses thresholds of 500 ms response time, 80% CPU, and 80% memory. It also
flags `WARNING` and `ERROR` log levels. The run detected two anomalies and did not flag
the eight normal records. No expected anomaly in the supplied data was missed.

The detector's main limitation is its use of fixed thresholds. It does not learn a
service baseline or account for time-of-day variation, so a gradual performance change
below a threshold could be missed. A possible improvement is a rolling baseline with
configurable alert sensitivity and correlation of repeated failures.

## Event-Processing Flow

The workflow is:

```text
Operational data
  -> AnomalyDetector
  -> anomaly event
  -> EventProducer
  -> anomaly-events topic
  -> EventConsumer
  -> AIOps output
```

An event contains its timestamp, service, type, detection reasons, and original source
record. The producer publishes each event to the shared `anomaly-events` topic, and the
consumer receives the same events from that topic.

## Issues Found and Corrected

1. The detector only treated `WARNING` as a concerning log level. It now handles both
	`WARNING` and `ERROR` while preserving the existing detector architecture.
2. The producer and consumer were connected to different topics. They now share the
	`anomaly-events` topic, allowing published events to be consumed.
3. Stray non-Python text appended to `src/aiops_pipeline.py` was removed so the module
	can be imported and executed.

## Execution Result

Run from the repository root:

```bash
PYTHONPATH=src python3 src/aiops_pipeline.py
```

The final execution processed 10 records, detected 2 anomalies, and consumed 2 events.
The output identified the payment-service timeout at `10:05` and the database
connection timeout with high CPU and memory at `10:06`.

Validation was completed with:

```bash
python3 -m pytest -q
```

Result: `8 passed in 0.03s`.

## Reproduce the Demonstration

1. Clone or open this repository in GitHub Codespaces or VS Code.
2. Install dependencies with `pip install -r requirements.txt`.
3. Run the pipeline with `PYTHONPATH=src python3 src/aiops_pipeline.py`.
4. Run the tests with `python3 -m pytest -q`.
5. Review the detected-event output and the test result.

&copy; 2025 GitHub &bull; [Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/code_of_conduct.md) &bull; [MIT License](https://gh.io/mit)
