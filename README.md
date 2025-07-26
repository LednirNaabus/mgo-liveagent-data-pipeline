# LiveAgent Client Template

---

## To Do (As of 7/26/2025):

- [ ] Refactor again

    - [x] Create Data Extractor class

    - [ ] Add `settings.py` in `config/` directory

        - [ ] Add `Dev`, `Local`, and `Prod`

    - [ ] Create Logging

    - [x] Do something about parsing the tickets (`TicketAPIResponse`)

    - [ ] Fix ticket messages extraction

    - [x] Agents extraction

    - [ ] Users extraction

    - [x] BigQuery integration

- [ ] Use `pydantic` instead of `dataclasses` in `api/schemas/`

---

# Notes

1. `/process-tickets` -> Fetch from API then upload to BigQuery (`POST`)
1. `/tickets` -> Get from BigQuery (`GET`)