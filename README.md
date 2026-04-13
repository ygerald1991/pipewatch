# pipewatch

A lightweight CLI tool for monitoring and alerting on ETL pipeline health metrics.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Start monitoring a pipeline by pointing pipewatch at your metrics endpoint or log source:

```bash
pipewatch monitor --source postgresql://user:pass@localhost/mydb --interval 60
```

Set up an alert rule:

```bash
pipewatch alert add --metric row_count --threshold 0 --condition eq --notify slack
```

Check pipeline status at a glance:

```bash
pipewatch status
```

```
Pipeline        Status     Last Run         Rows Processed   Errors
--------------  ---------  ---------------  ---------------  ------
orders_etl      ✓ healthy  2 minutes ago    14,302           0
user_sync       ✗ failed   18 minutes ago   —                3
inventory_load  ✓ healthy  5 minutes ago    8,901            0
```

Run `pipewatch --help` for a full list of commands and options.

---

## Configuration

pipewatch looks for a config file at `~/.pipewatch/config.yaml`. A minimal example:

```yaml
default_interval: 60
notify:
  slack_webhook: https://hooks.slack.com/services/your/webhook/url
```

---

## License

MIT © 2024 Your Name