# flow

simple task orchestrator for running commands with dependencies.

*because airflow is too much and bash scripts are chaos*

## usage

create `pipeline.yaml`:
```yaml
tasks:
  - id: scrape
    run: curl -s https://api.github.com/users/octocat | jq .

  - id: extract
    run: python extract_features.py
    depends_on: [scrape]

  - id: train
    run: python train_model.py --epochs 100
    depends_on: [extract]

  - id: deploy
    run: docker build . && docker push registry/model:latest
    depends_on: [train]

  - id: notify
    run: curl -X POST https://hooks.slack.com/... -d "model deployed"
    depends_on: [deploy]
```

run it:
```bash
flow run pipeline.yaml
```

watch it run in parallel, retry on failures, and actually work.

## options

```bash
flow run pipeline.yaml --max-workers 2 --retries 1 --resume
```

check status:
```bash
flow status
```

## features

- runs tasks in dependency order (obviously)
- parallel execution of independent tasks
- retries when things break (they will)
- resume from failures (your laptop died, we get it)
- logs everything so you can debug at 2am

## install

```bash
pip install -r requirements.txt
```

install globally:
```bash
pip install -e .
flow run pipeline.yaml
```

no kubernetes required.