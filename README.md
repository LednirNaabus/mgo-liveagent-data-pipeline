# LiveAgent Client Template

---

## To Do (As of 7/30/2025):

- [ ] Refactor again

    - [x] Create Data Extractor class

    - [ ] Add `settings.py` in `config/` directory

        - [ ] Add `Dev`, `Local`, and `Prod`

    - [ ] Create Logging

    - [x] Do something about parsing the tickets (`TicketAPIResponse`)

    - [x] Fix ticket messages extraction

    - [x] Agents extraction

    - [ ] Users extraction

        - [ ] Users route

    - [x] BigQuery integration

- [ ] Use `pydantic` instead of `dataclasses` in `api/schemas/`

### **branch: `refactor-v1-ticket-messages-parsing`**

- [x] Parse ticket messages

    - Collect `userids`, then use that to check in `/agents` and `/users/{userID}` endpoint to determine who the sender and receiver of the message is

- [x] Work on `core/Users.py`

---

# Notes

1. `/process-tickets` -> Fetch from API then upload to BigQuery (`POST`)
1. `/tickets` -> Get from BigQuery (`GET`)