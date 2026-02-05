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

#### For branch `refactor-v1-add-174`

- [x] Fix date filter in tickets and ticket messages

#### For branch `refactor-v1-logs`

- [ ] Implement logs

    - [x] Capture new and existing ticket and ticket messages per run

    - [ ] Capture error logs per route

    - [x] Total tokens used

    - [x] Model used

#### For branch `feat-add-236-tickets-and-messages`

- [x] Tickets route

- [x] Messages route

- [ ] Finalize return structure for methods in `ChannelAdapter` (`channel_gateway.py`)

- [ ] Restructure the routing in `api/`

    - [ ] Add validation

- [ ] Slot in the LLM in the pipeline

- [ ] Process conversation data (`ConvoData.py`)