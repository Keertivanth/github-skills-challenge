# AIOps Monitoring and Event Processing

This project is a lightweight AIOps simulation for the `payment-service`. It monitors
service telemetry, identifies abnormal behaviour, creates anomaly events, and moves
those events through an in-memory producer, topic, and consumer workflow.

## Scenario

The operational problem is detecting payment-service degradation early. Slow responses,
high resource usage, and error logs can indicate timeouts or database connectivity
problems. AIOps combines these signals into actionable anomaly events so that an
operations team can inspect one consistent result instead of reviewing raw records only.

## Task 1: Set Up and Understand the Environment

This work was completed in a GitHub Codespace using the default configuration. The
working copy is the fork at `Keertivanth/github-skills-challenge`; the original exercise
repository is configured separately as the `upstream` remote. The application keeps the
provided Python simulation structure and does not require Kafka, Airflow, or external
cloud services.

The main components are organized as follows:

| Assessment area | Repository component | Purpose |
| --- | --- | --- |
| Operational data | `data/service_data.json` | Synthetic payment-service records |
| Metrics and logs | `response_time_ms`, `cpu_percent`, `memory_percent`, `log_level`, `message` | Service measurements and log evidence in each record |
| Anomaly detection | `src/anomaly_detector.py` | Applies metric thresholds and log-level checks |
| Event production | `src/event_producer.py` | Publishes detected anomaly events |
| Event topic | `src/event_topic.py` | Stores events in an in-memory topic |
| Event consumption | `src/event_consumer.py` | Reads published events from the topic |
| Final AIOps processing | `src/aiops_pipeline.py` | Loads records, runs detection, and reports results |
| Validation | `tests/` | Checks detection and event delivery behaviour |

## Repository Components

- `data/service_data.json` contains the synthetic service telemetry.
- `src/anomaly_detector.py` applies thresholds and checks log levels.
- `src/event_producer.py` publishes detected events.
- `src/event_topic.py` provides the in-memory event topic.
- `src/event_consumer.py` reads events from the topic.
- `src/aiops_pipeline.py` loads the data and runs the end-to-end workflow.
- `tests/` validates detection and event delivery behaviour.

## Task 2: Analyse Logs and Metrics

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

## Task 3: Identify Anomalies

The provided `AnomalyDetector` was used without replacing its architecture. It processes
each record and applies thresholds of 500 ms response time, 80% CPU, and 80% memory. It
also identifies `WARNING` and `ERROR` log levels as concerning events.

The detection report identified these two anomalies:

- `2026-09-20T10:05:00`: `payment-service` had a 610 ms response time and an `ERROR`
  log stating `Payment service timeout`. Reasons: high response time and error log.
- `2026-09-20T10:06:00`: `payment-service` had a 640 ms response time, 94% CPU, and
  91% memory, with an `ERROR` log stating `Database connection timeout`. Reasons: high
  response time, high CPU utilization, high memory utilization, and error log.

The detector processed all 10 operational records, produced readable anomaly events
with timestamps, service names, reasons, and the original source records, and returned
no event for the eight normal `INFO` records. No expected anomaly was missed and no
normal observation was incorrectly flagged in the supplied data.

The detector's main limitation is its use of fixed thresholds. It does not learn a
service baseline or account for time-of-day variation, so a gradual performance change
below a threshold could be missed. A possible improvement is a rolling baseline with
configurable alert sensitivity and correlation of repeated failures.

## Task 4: Verify the AIOps Event Flow

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

The provided components have these roles:

- **Event/message:** The detector creates an anomaly event containing the timestamp,
  service, type, detection reasons, and original source record.
- **Producer:** `EventProducer` accepts each detected event and publishes it.
- **Topic:** `EventTopic("anomaly-events")` stores the published events in memory.
- **Consumer:** `EventConsumer` reads the events from the same topic.
- **Downstream AIOps output:** `run_pipeline` returns the consumed events in its
  `events_consumed` result field.

The complete flow was verified as follows:

1. The detector identified two anomalous records and created two `ANOMALY` events.
2. Each event was passed to `EventProducer.publish`.
3. The producer published both events to the shared `anomaly-events` topic.
4. `EventConsumer` read both events from that topic.
5. The consumer returned the event messages with their original details and reasons.
6. The pipeline returned both consumed events as the downstream AIOps output.

The execution result was 10 records processed, 2 anomalies detected, and 2 events
consumed. This confirms that an anomaly travels through the complete simulated
producer-to-topic-to-consumer workflow.

## Task 5: Investigate and Correct the Workflow

The initial assessment environment contained three workflow issues. Each correction
kept the existing detector, producer, topic, consumer, and pipeline architecture.

| Affected component | Cause | Correction and verification |
| --- | --- | --- |
| `src/anomaly_detector.py` | The log check treated `WARNING` as the only concerning log level, so `ERROR` records were not explicitly handled by the log rule. | The detector now handles both `WARNING` and `ERROR`. Re-running the pipeline reports `Error log detected` for both anomalous records. |
| `src/aiops_pipeline.py` producer/consumer wiring | The producer was created with `service-events`, while the consumer was created with a separate `anomaly-events` topic. Published events could not reach that consumer. | Both components now use the same `anomaly-events` topic. Re-running the pipeline produces 2 events and consumes the same 2 events. |
| `src/aiops_pipeline.py` source integrity | Stray non-Python text had been appended after the final output statement. | The appended text was removed. The module now imports and executes successfully. |

After each correction, the affected workflow was executed again. The final verification
processed 10 records, detected 2 anomalies, consumed 2 events, and printed both
payment-service incidents with their detection reasons. The test suite also completed
successfully with 8 passing tests.

## Task 6: Execute the End-to-End Pipeline

Run from the repository root:

```bash
PYTHONPATH=src python3 src/aiops_pipeline.py
```

The final execution processed 10 records, detected 2 anomalies, and consumed 2 events.
The end-to-end requirements were verified as follows:

1. Operational data was loaded and 10 records were processed.
2. Abnormal behaviour was detected in the records at `10:05` and `10:06`.
3. Two `ANOMALY` events were generated with timestamps, services, reasons, and source
  records.
4. The events were published by `EventProducer` to `anomaly-events`.
5. Both published events were consumed from the topic by `EventConsumer`.
6. The consumed event messages were returned successfully in `events_consumed`.
7. The final output represented the payment-service timeout and database connection
  timeout, including the high CPU and memory evidence for the second incident.

## Task 8: Run the Provided Validation

The provided validation was run from the repository root with:

```bash
python3 -m pytest -q
```

All 8 tests passed. The validation confirms that:

- the operational data can be loaded and processed;
- normal records are not reported as anomalies;
- abnormal metrics and concerning logs generate `ANOMALY` events;
- events move through the producer and `anomaly-events` topic;
- the consumer receives the generated events; and
- the complete AIOps pipeline finishes with the expected output.

The end-to-end execution was also rerun with:

```bash
PYTHONPATH=src python3 src/aiops_pipeline.py
```

It completed successfully with 10 records processed, 2 anomalies detected, and 2
events consumed. No validation failures remained before submission.

Validation was completed with:

```bash
python3 -m pytest -q
```

Result: `8 passed in 0.03s`.

## Task 7: Document Findings and Reproduce the Demonstration

This README contains the complete written assessment record: the AIOps scenario,
operational data description, log and metric observations, anomaly-detection findings,
event-processing flow, final execution result, corrected workflow issues, and a known
limitation with a possible improvement. The steps below allow another user to reproduce
the demonstration without relying on screenshots.

## Reproduce the Demonstration

1. Clone or open this repository in GitHub Codespaces or VS Code.
2. Install dependencies with `pip install -r requirements.txt`.
3. Run the pipeline with `PYTHONPATH=src python3 src/aiops_pipeline.py`.
4. Run the tests with `python3 -m pytest -q`.
5. Review the detected-event output and the test result.

## Task 9: Commit and Push the Submission

Before submission, the repository was reviewed to ensure that the changes are limited
to the relevant AIOps source corrections and assessment documentation. The validation
suite and end-to-end pipeline were run successfully, then the changes were committed
with meaningful messages and pushed to the fork at
`https://github.com/Keertivanth/github-skills-challenge`.

The final branch is `main`, and the latest commit is available on `origin/main`. The
repository is ready for final submission and pull-request creation.

## Final Submission Checklist

- Repository: [Keertivanth/github-skills-challenge](https://github.com/Keertivanth/github-skills-challenge)
- Branch: `main`, pushed to `origin/main`
- Validation: `8 passed`
- Final workflow: 10 records processed, 2 anomalies detected, 2 events consumed
- Pull request: create a PR from the fork's `main` branch to the original exercise repository
- Evidence to submit: operational data, detection output, event flow, final AIOps output, and successful test execution

&copy; 2025 GitHub &bull; [Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/code_of_conduct.md) &bull; [MIT License](https://gh.io/mit)
