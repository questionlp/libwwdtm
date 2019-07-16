# TODO

This document provides a list of tasks that need to be researched and/or
completed for specific API service stories, filling in feature gaps
required for the next version of the
[Wait Wait... Don't Tell Me! Stats Page](https://wwdt.me), taking care of
technical debts (such as: performance improvements, code clean-up), or simply
nice-to-have items.

## API Service

- TBD

## Stats Page

### Panelists

- Retrieve a list of shows and whether a panelist had the correct Bluff the Listener story or not and whether the panelist was chosen

### Shows

- Retrieve a list of months available for a specified year

## Technical Debts

- Break up `guest`, `host`, `location`, `panelist`, `scorekeeper` and `show` modules into smaller modules
- Look at the possibility of creating classes with slots to store data instead of using OrderedDicts
- Look at ways to bypass the need to query the database every time multiple objects need to be returned

## Miscellaneous

- TBD