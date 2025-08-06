# MechaniGo and Live Agent Data Pipeline for Convo Analysis

A streamlined conversation data extraction solution from Live Agent API for chat analysis using FastAPI and OpenAI API.

## Local Use

- To run locally, install the dependencies first:

```
pip install -r requirements.txt
```

- Configure your secrets:

    - [ ] OpenAI API key

    - [ ] Live Agent API key

    - [ ] Google API key

- Run locally using the following command:

```
python main.py
```

# Documentation

- Access the documentation for the pipeline by entering the URL in your browser of choice:

```
localhost:8080/docs
```

### TODO:
For branch `refactor-v1-add-174`

- [x] Fix date filter in tickets and ticket messages

For branch `refactor-v1-logs`

- [ ] Implement logs

    **Schema**:

    ```
    extraction_date, "DATETIME", i.e., 2025-07-08T15:02:33
    extraction_run_time, "FLOAT" i.e., 127.22, etc.
    no_tickets_new, "INTEGER"
    no_tickets_update, "INTEGER"
    no_tickets_total, "INTEGER" (new+update)
    no_messages_new, "INTEGER"
    no_messages_old, "INTEGER"
    no_messages_total, "INTEGER" (new+old)
    total_tokens, "INTEGER"
    model, "STRING"
    log_message, "STRING"
    ```

    - [x] Capture new and existing ticket and ticket messages per run

    - [ ] Capture error logs per route

    - [x] Total tokens used

    - [x] Model used