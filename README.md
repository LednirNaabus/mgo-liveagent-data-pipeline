# LiveAgent Client Template

---

## To Do:

- [ ] Create Logging

- [x] Create Data Extractor class

- [ ] Add `settings.py` in `config/` directory

    - [ ] Add `Dev`, `Local`, and `Prod`

---

## To Do (As of 7/25/2025):

- [ ] Fix ticket messages extraction

- [ ] Agents extraction

- [ ] Users extraction

- [ ] BigQuery integration

- [ ] Refactor again

    - [ ] Do something about parsing the tickets (`TicketAPIResponse`)

---

# Notes

1. `/process-tickets` -> Fetch from API then upload to BigQuery (`POST`)
1. `/tickets` -> Get from BigQuery (`GET`)